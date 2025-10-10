use std::{pin::Pin, sync::Arc, time::Duration};

use crate::entity::prelude::*;
use anyhow::Result;
use chrono::Local;
use entity::{workflow, workflow_process, workflow_timer};
use local_ip_address::local_ip;
use redis::{
    AsyncCommands, from_redis_value,
    streams::{StreamMaxlen, StreamReadOptions, StreamReadReply},
};
use redis_macros::{FromRedisValue, ToRedisArgs};
use sea_orm::{ActiveValue::Set, ColumnTrait};
use sea_orm::{EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use tokio::{sync::RwLock, time::sleep};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info, warn};

use crate::{logic::workflow::WorkflowLogic, state::AppState};

#[derive(Serialize, Deserialize, FromRedisValue, ToRedisArgs, Clone)]
pub enum WorkflowTimerTask {
    StartTimer(u64),
    StopTimer(String),
    DispatchWorkflow(String),
}

impl<'a> WorkflowLogic<'a> {
    async fn check_due_tasks(&self) -> Result<()> {
        let now = Local::now().timestamp();

        let redis_client = self.ctx.redis();
        let mut conn = redis_client.get_multiplexed_async_connection().await?;
        let key = "jiascheduler:workflow:delayed";
        let due_tasks: Vec<String> = conn.zrangebyscore(key, 0.0, now).await?;

        if !due_tasks.is_empty() {
            let _: () = conn.zrembyscore(key, 0.0, now).await?;

            // 3. 将每个任务发布到 channel
            for task_str in due_tasks {
                // let _: () = conn.publish("jobs:ready", &task_str)?;
            }
        }
        Ok(())
    }

    async fn schedule(&self, task: String) -> Result<()> {
        let mut sched = JobScheduler::new().await?;
        let job = Job::new_async("1/7 * * * * *", |uuid, mut l| {
            Box::pin(async move {
                println!("I run async every 7 seconds");

                // Query the next execution time for this job
                let next_tick = l.next_tick_for_job(uuid).await;
                match next_tick {
                    Ok(Some(ts)) => println!("Next time for 7s job is {:?}", ts),
                    _ => println!("Could not get next tick for 7s job"),
                }
            })
        })?;

        todo!();
    }

    pub async fn new_scheduler(&self) -> Result<JobScheduler> {
        let sched = JobScheduler::new().await?;
        sched.start().await?;
        // let job = Job::new_async("1/7 * * * * *", |uuid, mut l| {
        //     Box::pin(async move {
        //         println!("I run async every 7 seconds");

        //         // Query the next execution time for this job
        //         let next_tick = l.next_tick_for_job(uuid).await;
        //         match next_tick {
        //             Ok(Some(ts)) => println!("Next time for 7s job is {:?}", ts),
        //             _ => println!("Could not get next tick for 7s job"),
        //         }
        //     })
        // })?;

        // todo!();
        //
        Ok(sched)
    }

    pub async fn recv_timer_msg(
        &self,
        is_continue: Arc<RwLock<bool>>,
        mut cb: impl Sync
        + Send
        + FnMut(
            String,
            WorkflowTimerTask,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    ) -> Result<()> {
        if !*is_continue.read().await {
            return Ok(());
        }

        let redis_client = self.ctx.redis();
        let mut conn = redis_client.get_multiplexed_async_connection().await?;

        let ret: String = conn
            .xgroup_create_mkstream(Self::WORKFLOW_TIMER_TOPIC, Self::CONSUMER_GROUP, "$")
            .await
            .map_or_else(
                |e| {
                    warn!("failed create workflow timer stream group - {}", e);
                    "".to_string()
                },
                |v| v,
            );

        info!("create workflow timer stream group {}", ret);

        let opts = StreamReadOptions::default()
            .group(Self::CONSUMER_GROUP, local_ip()?.to_string())
            .block(100)
            .count(100);

        loop {
            if !*is_continue.read().await {
                return Ok(());
            }
            let ret: StreamReadReply = conn
                .xread_options(&[Self::WORKFLOW_TIMER_TOPIC], &[">"], &opts)
                .await?;

            if let Err(e) = conn
                .xtrim::<_, u64>(Self::WORKFLOW_TIMER_TOPIC, StreamMaxlen::Equals(5000))
                .await
            {
                error!("failed to trim workflow timer stream - {e}");
            };

            for stream_key in ret.keys {
                let msg_key = stream_key.key;

                for stream_id in stream_key.ids {
                    for (k, v) in stream_id.map {
                        let ret = match from_redis_value::<WorkflowTimerTask>(&v) {
                            Ok(msg) => cb(k, msg).await,
                            Err(e) => {
                                error!("failed to parse workflow timer val - {e}");
                                Ok(())
                            }
                        };

                        if let Err(e) = ret {
                            error!("failed to handle workflow timer msg - {e}");
                        }

                        let _: i32 = conn
                            .xack(
                                msg_key.clone(),
                                Self::CONSUMER_GROUP,
                                &[stream_id.id.clone()],
                            )
                            .await
                            .map_or_else(
                                |v| {
                                    error!("faile to exec workflow timer msg xack - {}", v);
                                    0
                                },
                                |v| v,
                            );
                    }
                }
            }
        }
    }

    async fn send_timer_msg<'b>(&self, msg: WorkflowTimerTask) -> Result<String> {
        let data = &[("t", msg)];

        let mut conn = self.ctx.redis().get_multiplexed_async_connection().await?;
        let v: String = conn.xadd(Self::WORKFLOW_TIMER_TOPIC, "*", data).await?;
        Ok(v)
    }

    pub async fn start_timer(&self, timer_id: u64, mut sched: JobScheduler) -> Result<()> {
        let timer_record = WorkflowTimer::find()
            .filter(workflow_timer::Column::Id.eq(timer_id))
            .one(&self.ctx.db)
            .await?;

        let Some(timer_record) = timer_record else {
            return Ok(());
        };

        let job = Job::new_async(timer_record.timer_expr, |uuid, mut l| {
            Box::pin(async move {
                println!("I run async every 7 seconds");
                // Query the next execution time for this job
                let next_tick = l.next_tick_for_job(uuid).await;
                match next_tick {
                    Ok(Some(ts)) => println!("Next time for 7s job is {:?}", ts),
                    _ => println!("Could not get next tick for 7s job"),
                }
            })
        })?;
        WorkflowTimer::update(workflow_timer::ActiveModel {
            id: Set(timer_id),
            schedule_guid: Set(job.guid().to_string()),
            ..Default::default()
        })
        .exec(&self.ctx.db)
        .await?;
        sched.add(job).await?;

        Ok(())
    }
}
