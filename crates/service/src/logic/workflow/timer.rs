use anyhow::Result;
use chrono::Local;
use redis::AsyncCommands;
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::logic::workflow::WorkflowLogic;

pub struct WorkflowTask {
    workflow_id: u64,
    version: String,
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
}
