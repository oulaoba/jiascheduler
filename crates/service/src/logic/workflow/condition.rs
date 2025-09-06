use crate::logic::workflow::condition;
use crate::logic::workflow::types::WorkflowNode;
use crate::{entity::prelude::*, state::AppContext};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use entity::workflow_process_node;
use expr::{Context, Environment};

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
        let mut outer_ctx = Context::default();
        let mut env = Environment::new();

        // compute(op,val,val2,ture)
        env.add_function("compute", |c| {
            let mut sum = 0;

            let op = c.args[0].as_string().unwrap();
            let v1 = c.args.get(1).unwrap();
            let v2 = c.args.get(2).unwrap();
            let is_all = c.args[3].as_bool().unwrap();

            match op {
                ">" => match v1 {
                    expr::Value::Number(_) => todo!(),
                    expr::Value::Bool(_) => todo!(),
                    expr::Value::Float(_) => todo!(),
                    expr::Value::Nil => todo!(),
                    expr::Value::String(_) => todo!(),
                    expr::Value::Array(values) => todo!(),
                    expr::Value::Map(index_map) => todo!(),
                },
                _ => todo!(),
            }

            Ok(sum.into())
        });

        if let Some(vars) = node.user_variables.as_object() {
            vars.iter().for_each(|(k, v)| {
                let v = v.clone();
                global_ctx.insert(k.to_string(), v.to_string());
            });
        }

        for rule in self.rules.iter() {
            let mut ctx = global_ctx.clone();
            let d = match rule.left_val.val_type {
                condition::ConditionValType::UserVariables => match rule.right_val.val_type {
                    condition::ConditionValType::UserVariables => {
                        if rule.op == "contains" {
                            format!("indexOf({}, {}) > 0", rule.left_val.val, rule.right_val.val)
                        } else {
                            format!("{} {} {}", rule.left_val.val, rule.op, rule.right_val.val)
                        }
                    }
                    condition::ConditionValType::Custom => {
                        ctx.insert("right_val", rule.right_val.val.clone());
                        if rule.op == "contains" {
                            format!("indexOf({}, right_val) > 0", rule.left_val.val)
                        } else {
                            format!("{} {} right_val", rule.left_val.val, rule.op)
                        }
                    }
                    condition::ConditionValType::ExitCode => {
                        let data = WorkflowProcessNodeTask::find()
                            .filter(workflow_process_node::Column::ProcessId.eq(&node.process_id))
                            .filter(workflow_process_node::Column::NodeId.eq(&node.current_node.id))
                            .all(&app_ctx.db)
                            .await?;
                        ctx.insert("right_val", expr::to_value(&data)?);
                        if rule.op == "contains" {
                            format!(
                                "all(right_val,{{indexOf(.exit_code,{}) > 0}})",
                                &rule.left_val.val,
                            )
                        } else {
                            format!(
                                "all(right_val,{{.exit_code {} {}}})",
                                rule.op, &rule.left_val.val,
                            )
                        }
                    }
                    condition::ConditionValType::Output => {
                        let data = WorkflowProcessNodeTask::find()
                            .filter(workflow_process_node::Column::ProcessId.eq(&node.process_id))
                            .filter(workflow_process_node::Column::NodeId.eq(&node.current_node.id))
                            .all(&app_ctx.db)
                            .await?;
                        ctx.insert("right_val", expr::to_value(&data)?);

                        if rule.op == "contains" {
                            format!(
                                "all(right_val,{{indexOf(.output,{}) > 0}})",
                                &rule.left_val.val,
                            )
                        } else {
                            format!(
                                "all(right_val,{{.output {} {}}})",
                                rule.op, &rule.left_val.val,
                            )
                        }
                    }
                },
                condition::ConditionValType::Custom => {
                    ctx.insert("left_value", rule.left_val.val.clone());
                    match rule.right_val.val_type {
                        condition::ConditionValType::UserVariables => {
                            if rule.op == "contains" {
                                format!("indexOf(left_val, {}) > 0", rule.right_val.val)
                            } else {
                                format!("left_value {} {}", rule.op, rule.right_val.val,)
                            }
                        }
                        condition::ConditionValType::Custom => {
                            ctx.insert("right_value", rule.right_val.val.clone());
                            if rule.op == "contains" {
                                format!("indexOf(left_val, {}) > 0", rule.right_val.val)
                            } else {
                                format!("left_value {} {}", rule.op, rule.right_val.val,)
                            }
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

                            ctx.insert("right_val", expr::to_value(&data)?);
                            if rule.op == "contains" {
                                format!("all(right_val,{{indexOf(.exit_code,left_val) > 0}})",)
                            } else {
                                format!("all(right_val,{{.exit_code {} left_val}})", rule.op)
                            }
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

                            ctx.insert("right_val", expr::to_value(&data)?);
                            if rule.op == "contains" {
                                format!("all(right_val,{{indexOf(.output,left_val) > 0}})",)
                            } else {
                                format!("all(right_val,{{.output {} left_val}})", rule.op)
                            }
                        }
                    }
                }
                condition::ConditionValType::ExitCode => {
                    let data = WorkflowProcessNodeTask::find()
                        .filter(workflow_process_node::Column::ProcessId.eq(&node.process_id))
                        .filter(workflow_process_node::Column::NodeId.eq(&node.current_node.id))
                        .all(&app_ctx.db)
                        .await?;

                    ctx.insert("left_val", expr::to_value(&data)?);
                    match rule.right_val.val_type {
                        condition::ConditionValType::UserVariables => {
                            if rule.op == "contains" {
                                format!(
                                    "all(left_val,{{indexOf(.exit_code, {}) > 0}})",
                                    rule.right_val.val
                                )
                            } else {
                                format!(
                                    "all(left_val,{{.exit_code {} {}}})",
                                    rule.op, rule.right_val.val
                                )
                            }
                        }
                        condition::ConditionValType::Custom => {
                            ctx.insert("right_val", &rule.right_val.val);
                            if rule.op == "contains" {
                                format!("all(left_val,{{indexOf(.exit_code, right_val) > 0}})",)
                            } else {
                                format!("all(left_val,{{.exit_code {} right_value}})", rule.op)
                            }
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

                    ctx.insert("left_val", expr::to_value(&data)?);
                    match rule.right_val.val_type {
                        condition::ConditionValType::UserVariables => {
                            if rule.op == "contains" {
                                format!(
                                    "all(left_val,{{indexOf(.output, {}) > 0}})",
                                    rule.right_val.val
                                )
                            } else {
                                format!(
                                    "all(left_val,{{.output {} {}}})",
                                    rule.op, rule.right_val.val
                                )
                            }
                        }
                        condition::ConditionValType::Custom => {
                            ctx.insert("right_val", &rule.right_val.val);
                            if rule.op == "contains" {
                                format!("all(left_val,{{indexOf(.output, right_val) > 0}})",)
                            } else {
                                format!("all(left_val,{{.output {} right_value}})", rule.op)
                            }
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
            let val = env
                .eval(d.as_str(), &ctx)?
                .as_bool()
                .ok_or(anyhow!("invalid express compare result"))?;

            outer_ctx.insert(rule.name.clone(), val);
        }

        expr::eval(&self.expr, &outer_ctx)?
            .as_bool()
            .ok_or(anyhow!("invalid express compare result"))
    }
}
