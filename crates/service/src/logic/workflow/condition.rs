use crate::IdGenerator;
use crate::logic::workflow::condition;
use crate::logic::workflow::types::WorkflowNode;
use crate::{entity::prelude::*, state::AppContext};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use entity::workflow_process_node;
use expr::Context;

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

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
    pub expr: String,
    pub rules: Vec<Rule>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Rule {
    pub name: String,
    pub left_val: ConditionVal,
    pub op: String,
    pub right_val: ConditionVal,
    // all or any
    pub compute_type: String,
}

impl Condition {
    pub async fn eval(&self, app_ctx: &AppContext, node: &WorkflowNode) -> Result<bool> {
        let mut global_ctx = Context::default();

        for rule in self.rules.iter() {
            let mut ctx = Context::default();
            let d = match rule.left_val.val_type {
                condition::ConditionValType::UserVariables => {
                    ctx.insert(&rule.left_val.val, "");
                    match rule.right_val.val_type {
                        condition::ConditionValType::UserVariables => {
                            ctx.insert(&rule.right_val.val, "");
                            format!("{} {} {}", rule.left_val.val, rule.op, rule.right_val.val)
                        }
                        condition::ConditionValType::Custom => {
                            format!("{} {} {}", rule.left_val.val, rule.op, rule.right_val.val)
                        }
                        condition::ConditionValType::ExitCode => {
                            let data = WorkflowProcessNodeTask::find()
                                .filter(
                                    workflow_process_node::Column::ProcessId.eq(&node.process_id),
                                )
                                .filter(
                                    workflow_process_node::Column::NodeId.eq(&node.current_node.id),
                                )
                                .all(&app_ctx.db)
                                .await?;

                            ctx.insert("node_task_result_arr", expr::to_value(&data)?);

                            format!(
                                "{}(node_task_result_arr, {{.exit_code {} {}}})",
                                &rule.compute_type, &rule.op, &rule.left_val.val,
                            )
                        }
                        condition::ConditionValType::Output => {
                            let data = WorkflowProcessNodeTask::find()
                                .filter(
                                    workflow_process_node::Column::ProcessId.eq(&node.process_id),
                                )
                                .filter(
                                    workflow_process_node::Column::NodeId.eq(&node.current_node.id),
                                )
                                .all(&app_ctx.db)
                                .await?;

                            ctx.insert("node_task_result_arr", expr::to_value(&data)?);
                            format!(
                                "{}(node_task_result_arr, {{.output {} {}}})",
                                &rule.compute_type, &rule.op, &rule.left_val.val,
                            )
                        }
                    }
                }
                condition::ConditionValType::Custom => match rule.right_val.val_type {
                    condition::ConditionValType::UserVariables => {
                        ctx.insert(&rule.right_val.val, "");
                        format!("{} {} {}", rule.left_val.val, rule.op, rule.right_val.val)
                    }
                    condition::ConditionValType::Custom => {
                        format!("{} {} {}", rule.left_val.val, rule.op, rule.right_val.val)
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
                            &rule.compute_type, &rule.op, &rule.left_val.val,
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
                            &rule.compute_type, &rule.op, &rule.left_val.val,
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
                    match rule.right_val.val_type {
                        condition::ConditionValType::UserVariables => {
                            ctx.insert(&rule.right_val.val, "");
                            format!(
                                "{}(node_task_result_arr, {{.exit_code {} {}}})",
                                &rule.compute_type, &rule.op, &rule.right_val.val,
                            )
                        }
                        condition::ConditionValType::Custom => {
                            ctx.insert("right_var", &rule.right_val.val);
                            format!(
                                "{}(node_task_result_arr, {{.exit_code {} right_var}})",
                                &rule.compute_type, &rule.op,
                            )
                        }
                        condition::ConditionValType::ExitCode => {
                            anyhow::bail!("not support exit code compare to exit code");
                        }
                        condition::ConditionValType::Output => {
                            anyhow::bail!("not support exit code compare to output");
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
                    match rule.right_val.val_type {
                        condition::ConditionValType::UserVariables => {
                            ctx.insert(&rule.right_val.val, "");
                            format!(
                                "{}(node_task_result_arr, {{.output {} {}}})",
                                &rule.compute_type, &rule.op, &rule.right_val.val
                            )
                        }
                        condition::ConditionValType::Custom => {
                            ctx.insert("right_var", &rule.right_val.val);
                            format!(
                                "{}(node_task_result_arr, {{.output {} right_var}})",
                                &rule.compute_type, &rule.op
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

            global_ctx.insert(rule.name.clone(), val);
        }

        expr::eval(&self.expr, &global_ctx)?
            .as_bool()
            .ok_or(anyhow!("invalid express compare result"))
    }
}
