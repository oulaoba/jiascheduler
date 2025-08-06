use anyhow::{Result, anyhow};
use redis_macros::{FromRedisValue, ToRedisArgs};
use sea_orm::{FromQueryResult, prelude::DateTimeLocal};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use core::matches;
use std::pin::Pin;

use crate::IdGenerator;
use crate::logic::job::types::{DispatchData, DispatchResult, DispatchTarget};
use crate::logic::types::UserInfo;
use crate::logic::workflow::condition;
use crate::logic::workflow::types::{
    self, CustomJob, NodeStatus, NodeType, ProcessStatus, StandardJob, Task, TaskType,
    WorkflowNode, WorkflowProcessArgs,
};
use crate::{
    entity::{prelude::*, team_member},
    state::AppContext,
};
use anyhow::{Result, anyhow};
use automate::bridge::msg::UpdateJobParams;
use automate::scheduler::types::{RunStatus, UploadFile};
use entity::{
    executor, instance, job, team, workflow, workflow_process, workflow_process_edge,
    workflow_process_node, workflow_process_node_task, workflow_version,
};
use expr::Context;
use local_ip_address::local_ip;
use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, from_redis_value};
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, JoinType, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait,
};
use sea_query::Expr;
use serde_json::json;
use tokio::fs;
use tracing::{debug, error, info, warn};
use utils::file_name;

use super::types::{EdgeConfig, NodeConfig};

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum ConditionValType {
    #[serde(rename = "user_variables")]
    UserVariables,
    #[serde(rename = "custom")]
    Custom,
    #[serde(rename = "exit_code")]
    ExitCode,
    #[serde(rename = "output")]
    Output,
}

impl Display for ConditionValType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConditionValType::UserVariables => write!(f, "user_variables"),
            ConditionValType::Custom => write!(f, "custom"),
            ConditionValType::ExitCode => write!(f, "exit_code"),
            ConditionValType::Output => write!(f, "output"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct ConditionVal {
    pub val_type: ConditionValType,
    pub val: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Condition {
    pub left_val: ConditionVal,
    pub op: String,
    pub right_val: ConditionVal,
    // all or any
    pub compute_type: String,
}

impl Condition {
    pub async fn eval(&self, app_ctx: &AppContext, node: &WorkflowNode) -> Result<bool> {
        let mut ctx = Context::default();

        let d = match self.left_val.val_type {
            condition::ConditionValType::UserVariables => {
                ctx.insert(&self.left_val.val, "");
                match self.right_val.val_type {
                    condition::ConditionValType::UserVariables => {
                        ctx.insert(&self.right_val.val, "");
                        format!("{} {} {}", self.left_val.val, self.op, self.right_val.val)
                    }
                    condition::ConditionValType::Custom => {
                        format!("{} {} {}", self.left_val.val, self.op, self.right_val.val)
                    }
                    condition::ConditionValType::ExitCode => {
                        let data = WorkflowProcessNodeTask::find()
                            .filter(workflow_process_node::Column::ProcessId.eq(&node.process_id))
                            .filter(workflow_process_node::Column::NodeId.eq(&node.current_node.id))
                            .all(&app_ctx.db)
                            .await?;

                        ctx.insert("node_task_result_arr", expr::to_value(&data)?);

                        format!(
                            "{}(node_task_result_arr, {{.exit_code {} {}}})",
                            &self.compute_type, &self.op, &self.left_val.val,
                        )
                    }
                    condition::ConditionValType::Output => {
                        let data = WorkflowProcessNodeTask::find()
                            .filter(workflow_process_node::Column::ProcessId.eq(&node.process_id))
                            .filter(workflow_process_node::Column::NodeId.eq(&node.current_node.id))
                            .all(&app_ctx.db)
                            .await?;

                        ctx.insert("node_task_result_arr", expr::to_value(&data)?);
                        format!(
                            "{}(node_task_result_arr, {{.output {} {}}})",
                            &self.compute_type, &self.op, &self.left_val.val,
                        )
                    }
                }
            }
            condition::ConditionValType::Custom => match self.right_val.val_type {
                condition::ConditionValType::UserVariables => {
                    ctx.insert(&self.right_val.val, "");
                    format!("{} {} {}", self.left_val.val, self.op, self.right_val.val)
                }
                condition::ConditionValType::Custom => {
                    format!("{} {} {}", self.left_val.val, self.op, self.right_val.val)
                }
                condition::ConditionValType::ExitCode => {
                    let data = WorkflowProcessNodeTask::find()
                        .filter(workflow_process_node::Column::ProcessId.eq(&node.process_id))
                        .filter(workflow_process_node::Column::NodeId.eq(&node.current_node.id))
                        .all(&app_ctx.db)
                        .await?;

                    ctx.insert("node_task_result_arr", expr::to_value(&data)?);
                    format!(
                        "{}(node_task_result_arr, {{.exit_code {} {}}})",
                        &self.compute_type, &self.op, &self.left_val.val,
                    )
                }
                condition::ConditionValType::Output => {
                    let data = WorkflowProcessNodeTask::find()
                        .filter(workflow_process_node::Column::ProcessId.eq(&node.process_id))
                        .filter(workflow_process_node::Column::NodeId.eq(&node.current_node.id))
                        .all(&app_ctx.db)
                        .await?;

                    ctx.insert("node_task_result_arr", expr::to_value(&data)?);
                    format!(
                        "{}(node_task_result_arr, {{.output {} {}}})",
                        &self.compute_type, &self.op, &self.left_val.val,
                    )
                }
            },
            condition::ConditionValType::ExitCode => {
                let data = WorkflowProcessNodeTask::find()
                    .filter(workflow_process_node::Column::ProcessId.eq(&node.process_id))
                    .filter(workflow_process_node::Column::NodeId.eq(&node.current_node.id))
                    .all(&app_ctx.db)
                    .await?;

                ctx.insert("node_task_result_arr", expr::to_value(&data)?);
                match self.right_val.val_type {
                    condition::ConditionValType::UserVariables => {
                        ctx.insert(&self.right_val.val, "");
                        format!(
                            "{}(node_task_result_arr, {{.exit_code {} {}}})",
                            &self.compute_type, &self.op, &self.right_val.val,
                        )
                    }
                    condition::ConditionValType::Custom => {
                        ctx.insert("right_var", &self.right_val.val);
                        format!(
                            "{}(node_task_result_arr, {{.exit_code {} right_var}})",
                            &self.compute_type, &self.op,
                        )
                    }
                    condition::ConditionValType::ExitCode => {
                        anyhow::bail!("not support exit code compare to exit code");
                    }
                    condition::ConditionValType::Output => {
                        anyhow::bail!("not support exit code compare to exit code");
                    }
                }
            }
            condition::ConditionValType::Output => {
                let data = WorkflowProcessNodeTask::find()
                    .filter(workflow_process_node::Column::ProcessId.eq(&node.process_id))
                    .filter(workflow_process_node::Column::NodeId.eq(&node.current_node.id))
                    .all(&app_ctx.db)
                    .await?;

                ctx.insert("node_task_result_arr", expr::to_value(&data)?);
                match self.right_val.val_type {
                    condition::ConditionValType::UserVariables => {
                        ctx.insert(&self.right_val.val, "");
                        format!(
                            "{}(node_task_result_arr, {{.output {} {}}})",
                            &self.compute_type, &self.op, &self.right_val.val
                        )
                    }
                    condition::ConditionValType::Custom => {
                        ctx.insert("right_var", &self.right_val.val);
                        format!(
                            "{}(node_task_result_arr, {{.output {} right_var}})",
                            &self.compute_type, &self.op
                        )
                    }
                    condition::ConditionValType::ExitCode => {
                        anyhow::bail!("not support output compare to exit code");
                    }
                    condition::ConditionValType::Output => {
                        anyhow::bail!("not support output compare to output");
                    }
                }
            }
        };
        let val = expr::eval(d.as_str(), &ctx)?
            .as_bool()
            .ok_or(anyhow!("invalid express compare result"))?;

        Ok(val)
    }
}
