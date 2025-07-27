use core::matches;
use std::pin::Pin;

use crate::logic::types::UserInfo;
use crate::logic::workflow::types::{
    self, CustomJob, NodeType, StandardJob, Task, TaskType, WorkflowNode, WorkflowProcessArgs,
};
use crate::{
    entity::{prelude::*, team_member},
    state::AppContext,
};
use anyhow::{Result, anyhow};
use automate::bus::Msg;
use entity::{team, workflow, workflow_process, workflow_version};
use local_ip_address::local_ip;
use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, from_redis_value};
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, JoinType, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait,
};
use sea_query::{Expr, any};
use serde_json::json;
use tracing::{debug, error, info, warn};

use super::types::{EdgeConfig, NodeConfig};

pub struct WorkflowLogic<'a> {
    ctx: &'a AppContext,
}

impl<'a> WorkflowLogic<'a> {
    pub const WORKFLOW_TOPIC: &'static str = "jiascheduler:workflow";
    pub const CONSUMER_GROUP: &'static str = "jiascheduler-group";

    pub fn new(ctx: &'a AppContext) -> Self {
        Self { ctx }
    }

    pub async fn get_workflow_list(
        &self,
        _user_info: &UserInfo,
        created_user: Option<&str>,
        default_id: Option<u64>,
        team_id: Option<u64>,
        name: Option<String>,
        page: u64,
        page_size: u64,
    ) -> Result<(Vec<types::WorkflowModel>, u64)> {
        let select = Workflow::find()
            .column_as(team::Column::Name, "team_name")
            .apply_if(team_id, |q, v| q.filter(workflow::Column::TeamId.eq(v)))
            .apply_if(name, |q, v| q.filter(workflow::Column::Name.contains(v)))
            .apply_if(created_user, |q, v| {
                q.filter(workflow::Column::CreatedUser.eq(v))
            })
            .join_rev(
                JoinType::LeftJoin,
                Team::belongs_to(Workflow)
                    .from(team::Column::Id)
                    .to(workflow::Column::TeamId)
                    .into(),
            );

        let total = select.clone().count(&self.ctx.db).await?;
        let ret = select
            .apply_if(default_id, |query, v| {
                query.order_by_desc(Expr::expr(workflow::Column::Id.eq(v)))
            })
            .order_by_desc(workflow::Column::Id)
            .into_model()
            .paginate(&self.ctx.db, page_size)
            .fetch_page(page - 1)
            .await?;

        Ok((ret, total))
    }

    pub async fn get_workflow_version_list(
        &self,
        _user_info: &UserInfo,
        version: Option<String>,
        created_user: Option<String>,
        workflow_id: u64,
        default_id: Option<u64>,
        page: u64,
        page_size: u64,
    ) -> Result<(Vec<workflow_version::Model>, u64)> {
        let select = WorkflowVersion::find()
            .filter(workflow_version::Column::WorkflowId.eq(workflow_id))
            .apply_if(created_user, |q, v| {
                q.filter(workflow_version::Column::CreatedUser.eq(v))
            })
            .apply_if(version, |q, v| {
                q.filter(workflow_version::Column::Version.contains(v))
            });

        let total = select.clone().count(&self.ctx.db).await?;
        let ret = select
            .apply_if(default_id, |query, v| {
                query.order_by_desc(Expr::expr(workflow::Column::Id.eq(v)))
            })
            .order_by_desc(workflow_version::Column::Id)
            .paginate(&self.ctx.db, page_size)
            .fetch_page(page - 1)
            .await?;

        Ok((ret, total))
    }

    pub async fn can_write_workflow(
        &self,
        user_info: &UserInfo,
        team_id: Option<u64>,
        workflow_id: Option<u64>,
    ) -> Result<bool> {
        let is_allowed = self.ctx.can_manage_job(&user_info.user_id).await?;
        if is_allowed {
            return Ok(true);
        }

        let is_team_user = if team_id.is_some() {
            TeamMember::find()
                .apply_if(team_id, |q, v| q.filter(team_member::Column::TeamId.eq(v)))
                .filter(team_member::Column::UserId.eq(&user_info.user_id))
                .one(&self.ctx.db)
                .await?
                .map(|_| true)
        } else {
            None
        };

        let Some(workflow_id) = workflow_id else {
            return Ok(is_team_user.is_some() || team_id == Some(0) || team_id.is_none());
        };

        let Some(workflow_record) = Workflow::find()
            .filter(workflow::Column::Id.eq(workflow_id))
            .one(&self.ctx.db)
            .await?
        else {
            return Ok(false);
        };

        if workflow_record.created_user == user_info.username {
            return Ok(true);
        }

        if is_team_user.is_some() {
            return Ok(Some(workflow_record.team_id) == team_id);
        }
        return Ok(TeamMember::find()
            .apply_if(Some(workflow_record.team_id), |q, v| {
                q.filter(team_member::Column::TeamId.eq(v))
            })
            .filter(team_member::Column::UserId.eq(&user_info.user_id))
            .one(&self.ctx.db)
            .await?
            .map(|_| true)
            == Some(true));
    }

    pub fn check_nodes(
        nodes: Option<Vec<NodeConfig>>,
        edges: Option<Vec<EdgeConfig>>,
    ) -> Result<(Option<Vec<NodeConfig>>, Option<Vec<EdgeConfig>>)> {
        let mut has_start_node = false;
        let mut has_end_node = false;

        let Some(nodes) = nodes else {
            return Ok((None, edges));
        };

        for node in &nodes {
            if node.task_type == TaskType::Standard
                && matches!(node.task, Task::Standard(StandardJob{ref eid}) if eid == "")
            {
                anyhow::bail!("no job is assigned to the workflow node {}", node.name)
            }
            if node.task_type == TaskType::Custom
                && matches!(node.task, Task::Custom(CustomJob{ executor_id, ref code, ..}) if executor_id == 0 || code == "")
            {
                anyhow::bail!(
                    "no custom task is assigned to the workflow node {}",
                    node.name
                )
            }

            if node.name == "" {
                anyhow::bail!("node name cannot be empty")
            }
            if node.id == "" {
                anyhow::bail!("node id cannot be empty")
            }

            if node.node_type == NodeType::StartEvent {
                has_start_node = true
            }

            if node.node_type == NodeType::EndEvent {
                has_end_node = true
            }
            let node_id = node.id.as_str();
            let is_connected = edges.as_ref().is_some_and(|v| {
                v.iter()
                    .any(|v| v.source_node_id == node_id || v.target_node_id == node_id)
            });

            if !is_connected {
                anyhow::bail!(
                    "{} is an isolated node. Please ensure the workflow is a valid directed acyclic graph (DAG)",
                    node.name
                )
            }
        }

        if !has_start_node {
            anyhow::bail!("workflow should has a start node")
        }
        if !has_end_node {
            anyhow::bail!("workflow should has a end node")
        }

        Ok((Some(nodes), edges))
    }

    pub async fn save_workflow(
        &self,
        id: Option<u64>,
        user_info: &UserInfo,
        name: String,
        info: Option<String>,
        team_id: Option<u64>,
        nodes: Option<Vec<NodeConfig>>,
        edges: Option<Vec<EdgeConfig>>,
    ) -> Result<u64> {
        let (nodes, edges) = Self::check_nodes(nodes, edges)?;
        let nodes = nodes
            .map(|v| serde_json::to_value(v))
            .transpose()?
            .map_or(NotSet, |v| Set(Some(v)));
        let edges = edges
            .map(|v| serde_json::to_value(v))
            .transpose()?
            .map_or(NotSet, |v| Set(Some(v)));

        let active_model = workflow::ActiveModel {
            id: id.map_or(NotSet, |v| Set(v)),
            name: Set(name),
            info: info.map_or(NotSet, |v| Set(v)),
            team_id: team_id.map_or(NotSet, |v| Set(v)),
            created_user: Set(user_info.username.clone()),
            updated_user: Set(user_info.username.clone()),
            nodes,
            edges,
            ..Default::default()
        };

        if let Some(id) = id {
            let affected = Workflow::update_many()
                .set(active_model)
                .filter(workflow::Column::Id.eq(id))
                .filter(workflow::Column::IsDeleted.eq(false))
                .exec(&self.ctx.db)
                .await?
                .rows_affected;
            return Ok(affected);
        }

        let active_model = active_model.save(&self.ctx.db).await?;
        Ok(active_model.id.as_ref().to_owned())
    }

    pub async fn release_version(
        &self,
        workflow_id: u64,
        user_info: &UserInfo,
        version: String,
        version_info: Option<String>,
        nodes: Option<Vec<NodeConfig>>,
        edges: Option<Vec<EdgeConfig>>,
        team_id: Option<u64>,
    ) -> Result<u64> {
        let (nodes, edges) = Self::check_nodes(nodes, edges)?;

        workflow::ActiveModel {
            id: Set(workflow_id),
            team_id: team_id.map_or(NotSet, |v| Set(v)),
            nodes: Set(nodes.clone().map(|v| serde_json::to_value(v)).transpose()?),
            edges: Set(edges.clone().map(|v| serde_json::to_value(v)).transpose()?),
            created_user: Set(user_info.username.clone()),
            updated_user: Set(user_info.username.clone()),
            ..Default::default()
        }
        .save(&self.ctx.db)
        .await?;

        let ret = workflow_version::ActiveModel {
            workflow_id: Set(workflow_id),
            team_id: team_id.map_or(NotSet, |v| Set(v)),
            version: Set(version),
            version_info: version_info.map_or(NotSet, |v| Set(v)),
            nodes: Set(nodes.map(|v| serde_json::to_value(v)).transpose()?),
            edges: Set(edges.map(|v| serde_json::to_value(v)).transpose()?),
            created_user: Set(user_info.username.clone()),
            ..Default::default()
        }
        .save(&self.ctx.db)
        .await?;

        Ok(ret.id.as_ref().to_owned())
    }

    pub async fn get_workflow_detail(
        &self,
        workflow_id: u64,
        version_id: Option<u64>,
    ) -> Result<types::WorkflowVersionDetailModel> {
        let workflow_record: types::WorkflowModel = Workflow::find()
            .filter(workflow::Column::Id.eq(workflow_id))
            .join_rev(
                JoinType::LeftJoin,
                Team::belongs_to(Workflow)
                    .from(team::Column::Id)
                    .to(workflow::Column::TeamId)
                    .into(),
            )
            .into_model()
            .one(&self.ctx.db)
            .await?
            .ok_or(anyhow!("not found workflow {}", workflow_id))?;

        let mut ret = types::WorkflowVersionDetailModel {
            workflow_id: workflow_id,
            workflow_name: workflow_record.name,
            workflow_info: workflow_record.info,
            nodes: workflow_record.nodes,
            edges: workflow_record.edges,
            team_id: workflow_record.team_id,
            created_user: workflow_record.created_user,
            updated_user: workflow_record.updated_user,
            created_time: workflow_record.created_time,
            updated_time: workflow_record.updated_time,
            ..Default::default()
        };

        let Some(version_id) = version_id else {
            return Ok(ret);
        };

        let version_record = WorkflowVersion::find()
            .filter(workflow_version::Column::Id.eq(version_id))
            .one(&self.ctx.db)
            .await?
            .ok_or(anyhow!("not found workflow version {}", version_id))?;

        ret.version = Some(version_record.version);
        ret.version_id = Some(version_record.id);
        ret.version_info = Some(version_record.version_info);
        ret.nodes = version_record.nodes;
        ret.edges = version_record.edges;

        Ok(ret)
    }

    async fn send_msg<'b>(&self, items: &'b [(&'b str, WorkflowNode)]) -> Result<String> {
        let mut conn = self.ctx.redis().get_multiplexed_async_connection().await?;
        let v: String = conn.xadd(Self::WORKFLOW_TOPIC, "*", items).await?;
        Ok(v)
    }

    pub async fn flow_next(&self, node: WorkflowNode) -> Result<String> {
        let val = self.send_msg(&[("workflow", node)]).await?;
        Ok(val)
    }

    pub async fn handle_start(&self, node: &WorkflowNode) -> Result<()> {
        let mut next_node = node.to_owned();
        next_node.reached_edge = Some(
            node.get_next_edge()
                .ok_or(anyhow!("not found next edge"))?
                .to_owned(),
        );
        next_node.current_node = node
            .get_next_node()
            .ok_or(anyhow!("not found next node"))?
            .to_owned();

        let _ = self.flow_next(next_node).await?;
        Ok(())
    }

    pub async fn handle_service_task(&self, node: &WorkflowNode) -> Result<()> {
        info!("{}", serde_json::to_string_pretty(&node)?);
        Ok(())
    }

    pub async fn process_node(&self, mut node: WorkflowNode) -> Result<()> {
        node.flow_depth += 1;

        let ret = match node.current_node.node_type {
            NodeType::StartEvent => self.handle_start(&node).await,
            NodeType::ServiceTask => self.handle_service_task(&node).await,
            NodeType::EndEvent => todo!(),
            NodeType::ExclusiveGateway => todo!(),
        };

        if let Err(e) = ret {
            error!(
                "failed handle workflow node {e}, node: {:?}",
                serde_json::to_string_pretty(&node).unwrap_or_default()
            );
        }

        Ok(())
    }

    pub async fn start_process(
        &self,
        user_info: &UserInfo,
        workflow_id: u64,
        version_id: u64,
        process_name: String,
        process_args: Option<WorkflowProcessArgs>,
    ) -> Result<String> {
        let version_record = WorkflowVersion::find()
            .filter(workflow_version::Column::WorkflowId.eq(workflow_id))
            .filter(workflow_version::Column::Id.eq(version_id))
            .one(&self.ctx.db)
            .await?
            .ok_or(anyhow!("not found workflow version record"))?;

        let nodes: Vec<NodeConfig> =
            serde_json::from_value(version_record.nodes.ok_or(anyhow!("invalid nodes data"))?)?;
        let edges: Vec<EdgeConfig> =
            serde_json::from_value(version_record.edges.ok_or(anyhow!("invalid edges data"))?)?;

        let start_node = nodes
            .iter()
            .find(|&v| v.node_type == NodeType::StartEvent)
            .ok_or(anyhow!("not found start node"))?
            .to_owned();
        let curr_node_id = start_node.id.clone();

        let process_id = nanoid::nanoid!();
        self.flow_next(WorkflowNode {
            process_id: process_id.clone(),
            origin_nodes: nodes,
            origin_edges: edges,
            user_variables: json!({}),
            process_args: None,
            eval_val: false,
            flow_depth: 0,
            actual_args: None,
            reached_edge: None,
            current_node: start_node,
        })
        .await?;

        WorkflowProcess::insert(workflow_process::ActiveModel {
            process_id: Set(process_id.clone()),
            process_name: Set(process_name),
            workflow_id: Set(workflow_id),
            version_id: Set(version_id),
            process_args: NotSet,
            process_status: NotSet,
            current_node: Set(curr_node_id),
            created_user: Set(user_info.username.clone()),
            ..Default::default()
        })
        .exec(&self.ctx.db)
        .await?;

        Ok(process_id)
    }

    pub async fn recv(
        &self,
        mut cb: impl Sync
        + Send
        + FnMut(String, WorkflowNode) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    ) -> Result<String> {
        let redis_client = self.ctx.redis();
        let mut conn = redis_client.get_multiplexed_async_connection().await?;

        let ret: String = conn
            .xgroup_create_mkstream(Self::WORKFLOW_TOPIC, Self::CONSUMER_GROUP, "$")
            .await
            .map_or_else(
                |e| {
                    warn!("failed create workflow stream group - {}", e);
                    "".to_string()
                },
                |v| v,
            );

        info!("create stream group {}", ret);

        let opts = StreamReadOptions::default()
            .group(Self::CONSUMER_GROUP, local_ip()?.to_string())
            .block(100)
            .count(100);

        loop {
            let ret: StreamReadReply = conn
                .xread_options(&[Self::WORKFLOW_TOPIC], &[">"], &opts)
                .await?;

            match conn
                .xtrim::<_, u64>(Self::WORKFLOW_TOPIC, StreamMaxlen::Equals(5000))
                .await
            {
                Ok(n) => debug!("trim stream {} {n} entries", Self::WORKFLOW_TOPIC),
                Err(e) => error!("failed to trim stream - {e}"),
            };

            for stream_key in ret.keys {
                let msg_key = stream_key.key;

                for stream_id in stream_key.ids {
                    for (k, v) in stream_id.map {
                        let ret = match from_redis_value::<WorkflowNode>(&v) {
                            Ok(msg) => cb(k, msg).await,
                            Err(e) => {
                                error!("failed to parse redis val - {e}");
                                Ok(())
                            }
                        };

                        if let Err(e) = ret {
                            error!("failed to handle msg - {e}");
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
                                    error!("faile to exec xack - {}", v);
                                    0
                                },
                                |v| v,
                            );
                    }
                }
            }
        }
    }
}
