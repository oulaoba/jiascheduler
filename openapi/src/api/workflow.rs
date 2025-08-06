use anyhow::{Context, Result};
use poem::{web::Data, Endpoint, EndpointExt};
use poem_openapi::{
    param::{Header, Query},
    payload::Json,
    OpenApi,
};
use service::logic::workflow::types::{EdgeConfig, NodeConfig};

use crate::{
    api::workflow::types::{StandardJob, TaskType},
    api_response, local_time, logic, middleware, return_err, return_ok,
    state::AppState,
};

mod types {
    use poem_openapi::{Enum, Object};
    use serde::{Deserialize, Serialize};
    use service::logic::{self, workflow::condition};
    use std::{collections::HashMap, fmt::Display};

    use crate::api::job;

    pub fn default_page() -> u64 {
        1
    }

    pub fn default_page_size() -> u64 {
        20
    }

    #[derive(Object, Deserialize, Serialize)]
    pub struct SaveWorkflowReq {
        pub id: Option<u64>,
        pub name: String,
        pub info: Option<String>,
        pub nodes: Option<Vec<NodeConfig>>,
        pub edges: Option<Vec<EdgeConfig>>,
    }

    #[derive(Object, Deserialize, Serialize)]
    pub struct SaveWorkflowResp {
        pub result: u64,
    }

    #[derive(Serialize, Enum, Deserialize, Clone)]
    pub enum NodeType {
        #[oai(rename = "bpmn:startEvent")]
        #[serde(rename = "bpmn:startEvent")]
        StartEvent,
        #[oai(rename = "bpmn:serviceTask")]
        #[serde(rename = "bpmn:serviceTask")]
        ServiceTask,
        #[oai(rename = "bpmn:endEvent")]
        #[serde(rename = "bpmn:endEvent")]
        EndEvent,
        #[oai(rename = "bpmn:exclusiveGateway")]
        #[serde(rename = "bpmn:exclusiveGateway")]
        ExclusiveGateway,
    }

    impl Display for NodeType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                NodeType::StartEvent => write!(f, "bpmn:startEvent"),
                NodeType::ServiceTask => write!(f, "bpmn:serviceTask"),
                NodeType::EndEvent => write!(f, "bpmn:endEvent"),
                NodeType::ExclusiveGateway => write!(f, "bpmn:exclusiveGateway"),
            }
        }
    }

    impl TryFrom<&str> for NodeType {
        type Error = anyhow::Error;
        fn try_from(value: &str) -> Result<Self, Self::Error> {
            match value {
                "bpmn:startEvent" => Ok(NodeType::StartEvent),
                "bpmn:serviceTask" => Ok(NodeType::ServiceTask),
                "bpmn:endEvent" => Ok(NodeType::EndEvent),
                "bpmn:exclusiveGateway" => Ok(NodeType::ExclusiveGateway),
                _ => Err(anyhow::anyhow!("Invalid node type")),
            }
        }
    }

    #[derive(Serialize, Object, Deserialize, Clone)]
    pub struct Task {
        pub standard: Option<StandardJob>,
        pub custom: Option<CustomJob>,
    }

    impl TryFrom<logic::workflow::types::Task> for Task {
        type Error = anyhow::Error;
        fn try_from(value: logic::workflow::types::Task) -> Result<Self, Self::Error> {
            let data = serde_json::to_string_pretty(&value)?;

            println!("here---{data:?}");
            Ok(match value {
                logic::workflow::types::Task::Standard(standard_job) => Self {
                    standard: Some(StandardJob {
                        eid: standard_job.eid,
                    }),
                    custom: None,
                },
                logic::workflow::types::Task::Custom(custom_job) => Self {
                    standard: None,
                    custom: Some(CustomJob {
                        executor_id: custom_job.executor_id,
                        timeout: custom_job.timeout,
                        code: custom_job.code,
                        upload_file: custom_job.upload_file,
                    }),
                },
                logic::workflow::types::Task::None => Self {
                    standard: None,
                    custom: None,
                },
            })
        }
    }

    impl TryInto<logic::workflow::types::Task> for Task {
        type Error = anyhow::Error;

        fn try_into(self) -> Result<logic::workflow::types::Task, Self::Error> {
            if let Some(std_job) = self.standard {
                Ok(logic::workflow::types::Task::Standard(
                    logic::workflow::types::StandardJob { eid: std_job.eid },
                ))
            } else if let Some(job) = self.custom {
                Ok(logic::workflow::types::Task::Custom(
                    logic::workflow::types::CustomJob {
                        executor_id: job.executor_id,
                        timeout: job.timeout,
                        code: job.code,
                        upload_file: job.upload_file,
                    },
                ))
            } else {
                Ok(logic::workflow::types::Task::None)
            }
        }
    }

    #[derive(Serialize, Enum, Deserialize, Clone, PartialEq)]
    pub enum TaskType {
        #[oai(rename = "standard")]
        #[serde(rename = "standard")]
        Standard,
        #[oai(rename = "custom")]
        #[serde(rename = "custom")]
        Custom,
        #[oai(rename = "none")]
        #[serde(rename = "none")]
        None,
    }

    impl Display for TaskType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                TaskType::Standard => write!(f, "standard"),
                TaskType::Custom => write!(f, "custom"),
                TaskType::None => write!(f, "none"),
            }
        }
    }

    impl TryFrom<&str> for TaskType {
        type Error = anyhow::Error;
        fn try_from(value: &str) -> Result<Self, Self::Error> {
            match value {
                "standard" => Ok(TaskType::Standard),
                "custom" => Ok(TaskType::Custom),
                "none" => Ok(TaskType::None),
                _ => Err(anyhow::anyhow!("Invalid task type")),
            }
        }
    }

    #[derive(Serialize, Object, Deserialize, Clone, Debug)]
    pub struct CustomJob {
        pub executor_id: u64,
        pub timeout: Option<u64>,
        pub code: String,
        pub upload_file: Option<String>,
    }

    #[derive(Serialize, Object, Deserialize, Clone, Debug)]
    pub struct StandardJob {
        pub eid: String,
    }

    #[derive(Clone, Object, Serialize, Deserialize)]
    pub struct NodeConfig {
        pub id: String,
        pub name: String,
        pub node_type: NodeType,
        pub task_type: TaskType,
        pub task: Task,
        pub data: serde_json::Value,
    }

    impl TryInto<logic::workflow::types::NodeConfig> for NodeConfig {
        type Error = anyhow::Error;
        fn try_into(self) -> Result<logic::workflow::types::NodeConfig, Self::Error> {
            Ok(logic::workflow::types::NodeConfig {
                id: self.id,
                name: self.name,
                node_type: self.node_type.to_string().as_str().try_into()?,
                task_type: self.task_type.to_string().as_str().try_into()?,
                task: self.task.try_into()?,
                data: self.data,
            })
        }
    }

    impl TryFrom<logic::workflow::types::NodeConfig> for NodeConfig {
        type Error = anyhow::Error;
        fn try_from(value: logic::workflow::types::NodeConfig) -> Result<Self, Self::Error> {
            Ok(NodeConfig {
                id: value.id,
                name: value.name,
                node_type: value.node_type.to_string().as_str().try_into()?,
                task_type: value.task_type.to_string().as_str().try_into()?,
                task: value.task.try_into()?,
                data: value.data,
            })
        }
    }

    #[derive(Clone, Object, Serialize, Deserialize)]
    pub struct EdgeConfig {
        pub id: String,
        pub name: String,
        pub conditions: Vec<Condition>,
        pub source_node_id: String,
        pub target_node_id: String,
        pub data: serde_json::Value,
    }

    #[derive(Serialize, Enum, Deserialize, Clone, PartialEq)]
    pub enum ConditionValType {
        #[oai(rename = "user_variables")]
        #[serde(rename = "user_variables")]
        UserVariables,
        #[oai(rename = "custom")]
        #[serde(rename = "custom")]
        Custom,
        #[oai(rename = "exit_code")]
        #[serde(rename = "exit_code")]
        ExitCode,
        #[oai(rename = "output")]
        #[serde(rename = "output")]
        Output,
    }

    impl From<condition::ConditionValType> for ConditionValType {
        fn from(value: condition::ConditionValType) -> Self {
            match value {
                condition::ConditionValType::UserVariables => ConditionValType::UserVariables,
                condition::ConditionValType::Custom => ConditionValType::Custom,
                condition::ConditionValType::ExitCode => ConditionValType::ExitCode,
                condition::ConditionValType::Output => ConditionValType::Output,
            }
        }
    }

    impl Into<condition::ConditionValType> for ConditionValType {
        fn into(self) -> condition::ConditionValType {
            match self {
                ConditionValType::UserVariables => condition::ConditionValType::UserVariables,
                ConditionValType::Custom => condition::ConditionValType::Custom,
                ConditionValType::ExitCode => condition::ConditionValType::ExitCode,
                ConditionValType::Output => condition::ConditionValType::Output,
            }
        }
    }

    #[derive(Serialize, Object, Deserialize, Clone, PartialEq)]
    pub struct ConditionVal {
        pub val_type: ConditionValType,
        pub val: String,
    }

    #[derive(Clone, Object, Serialize, Deserialize)]
    pub struct Condition {
        pub left_val: ConditionVal,
        pub op: String,
        pub right_val: ConditionVal,
        pub compute_type: String,
    }

    impl TryInto<logic::workflow::types::EdgeConfig> for EdgeConfig {
        type Error = anyhow::Error;
        fn try_into(self) -> Result<logic::workflow::types::EdgeConfig, Self::Error> {
            Ok(logic::workflow::types::EdgeConfig {
                id: self.id,
                name: self.name,
                conditions: self
                    .conditions
                    .iter()
                    .map(|v| condition::Condition {
                        left_val: condition::ConditionVal {
                            val_type: v.left_val.val_type.clone().into(),
                            val: v.left_val.val.to_string(),
                        },
                        op: v.op.to_string(),
                        compute_type: v.compute_type.clone(),
                        right_val: condition::ConditionVal {
                            val_type: v.right_val.val_type.clone().into(),
                            val: v.left_val.val.to_string(),
                        },
                    })
                    .collect(),
                source_node_id: self.source_node_id,
                target_node_id: self.target_node_id,
                data: self.data,
            })
        }
    }

    impl TryFrom<logic::workflow::types::EdgeConfig> for EdgeConfig {
        type Error = anyhow::Error;
        fn try_from(value: logic::workflow::types::EdgeConfig) -> Result<Self, Self::Error> {
            Ok(EdgeConfig {
                id: value.id,
                name: value.name,
                conditions: value
                    .conditions
                    .iter()
                    .map(|v| Condition {
                        left_val: ConditionVal {
                            val_type: v.left_val.val_type.clone().into(),
                            val: v.left_val.val.to_string(),
                        },
                        op: v.op.clone(),
                        compute_type: v.compute_type.clone(),
                        right_val: ConditionVal {
                            val_type: v.right_val.val_type.clone().into(),
                            val: v.right_val.val.to_string(),
                        },
                    })
                    .collect(),
                source_node_id: value.source_node_id,
                target_node_id: value.target_node_id,
                data: value.data,
            })
        }
    }

    #[derive(Object, Deserialize, Serialize)]
    pub struct ReleaseWorkflowVersionReq {
        pub workflow_id: u64,
        pub version: String,
        pub version_info: Option<String>,
        pub nodes: Option<Vec<NodeConfig>>,
        pub edges: Option<Vec<EdgeConfig>>,
    }

    #[derive(Object, Deserialize, Serialize)]
    pub struct SaveWorkflowVersionResp {
        pub result: u64,
    }

    #[derive(Object, Serialize, Default)]
    pub struct QueryWorkflowResp {
        pub total: u64,
        pub list: Vec<WorkflowRecord>,
    }

    #[derive(Object, Serialize, Default)]
    pub struct ResourceTag {
        pub id: u64,
        pub tag_name: String,
    }

    #[derive(Object, Serialize, Default)]
    pub struct WorkflowRecord {
        pub id: u64,
        pub name: String,
        pub info: String,
        pub tags: Option<Vec<ResourceTag>>,
        pub team_name: Option<String>,
        pub team_id: u64,
        pub updated_time: String,
        pub created_user: String,
    }

    #[derive(Object, Serialize, Default)]
    pub struct QueryWorkflowVersionResp {
        pub total: u64,
        pub list: Vec<WorkflowVersionRecord>,
    }

    #[derive(Object, Serialize, Default)]
    pub struct WorkflowVersionRecord {
        pub id: u64,
        pub workflow_id: u64,
        pub version: String,
        pub version_info: String,
        pub created_time: String,
        pub created_user: String,
        pub nodes: Option<Vec<NodeConfig>>,
        pub edges: Option<Vec<EdgeConfig>>,
    }

    #[derive(Object, Serialize, Default)]
    pub struct GetWorkflowDetailResp {
        pub workflow_id: u64,
        pub version_id: Option<u64>,
        pub workflow_name: String,
        pub workflow_info: String,
        pub version: Option<String>,
        pub version_info: Option<String>,
        pub updated_time: String,
        pub created_user: String,
        pub nodes: Option<Vec<NodeConfig>>,
        pub edges: Option<Vec<EdgeConfig>>,
    }

    #[derive(Object, Serialize, Default)]
    pub struct StartProcessReq {
        pub workflow_id: u64,
        pub version_id: u64,
        pub process_name: String,
        pub process_args: Option<WorkflowProcessArgs>,
    }

    #[derive(Default, Serialize, Deserialize, Object)]
    pub struct WorkflowNodeArgs {
        pub node_id: String,
        pub target: Vec<String>,
        pub args: serde_json::Value,
    }

    #[derive(Default, Serialize, Deserialize, Object)]
    pub struct WorkflowProcessArgs {
        pub default_target: Option<Vec<String>>,
        pub user_variables: Option<serde_json::Value>,
        pub nodes: Option<Vec<WorkflowNodeArgs>>,
    }

    #[derive(Object, Serialize, Default)]
    pub struct StartProcessResp {
        pub process_id: String,
    }
}

fn set_middleware(ep: impl Endpoint) -> impl Endpoint {
    ep.with(middleware::TeamPermissionMiddleware)
}
pub struct WorkflowApi;

#[OpenApi(prefix_path = "/workflow", tag = super::Tag::Team)]
impl WorkflowApi {
    #[oai(path = "/save", method = "post")]
    pub async fn save_workflow(
        &self,
        state: Data<&AppState>,
        user_info: Data<&logic::types::UserInfo>,
        Json(req): Json<types::SaveWorkflowReq>,
        #[oai(name = "X-Team-Id")] Header(team_id): Header<Option<u64>>,
    ) -> api_response!(types::SaveWorkflowResp) {
        let svc = state.service();
        if !svc
            .workflow
            .can_write_workflow(&user_info, team_id, req.id)
            .await?
        {
            return_err!("no permission");
        }

        let nodes: Option<Vec<logic::workflow::types::NodeConfig>> = req
            .nodes
            .map(|v| v.into_iter().map(|v| v.try_into()).collect())
            .transpose()?;
        let edges: Option<Vec<logic::workflow::types::EdgeConfig>> = req
            .edges
            .map(|v| v.into_iter().map(|v| v.try_into()).collect())
            .transpose()?;

        let ret = svc
            .workflow
            .save_workflow(
                req.id, &user_info, req.name, req.info, team_id, nodes, edges,
            )
            .await?;

        return_ok!(types::SaveWorkflowResp { result: ret })
    }

    #[oai(path = "/release", method = "post")]
    pub async fn release_version(
        &self,
        state: Data<&AppState>,
        user_info: Data<&logic::types::UserInfo>,
        Json(req): Json<types::ReleaseWorkflowVersionReq>,
        #[oai(name = "X-Team-Id")] Header(team_id): Header<Option<u64>>,
    ) -> api_response!(types::SaveWorkflowVersionResp) {
        let svc = state.service();
        if !svc
            .workflow
            .can_write_workflow(&user_info, team_id, Some(req.workflow_id))
            .await?
        {
            return_err!("no permission");
        }

        let nodes: Option<Vec<logic::workflow::types::NodeConfig>> = req
            .nodes
            .map(|v| v.into_iter().map(|v| v.try_into()).collect())
            .transpose()?;
        let edges: Option<Vec<logic::workflow::types::EdgeConfig>> = req
            .edges
            .map(|v| v.into_iter().map(|v| v.try_into()).collect())
            .transpose()?;
        let ret = svc
            .workflow
            .release_version(
                req.workflow_id,
                &user_info,
                req.version,
                req.version_info,
                nodes,
                edges,
                team_id,
            )
            .await?;

        return_ok!(types::SaveWorkflowVersionResp { result: ret })
    }

    #[oai(path = "/list", method = "get", transform = "set_middleware")]
    pub async fn query_workflow(
        &self,
        state: Data<&AppState>,
        user_info: Data<&logic::types::UserInfo>,
        #[oai(default = "types::default_page", validator(maximum(value = "10000")))]
        Query(page): Query<u64>,
        #[oai(
            default = "types::default_page_size",
            validator(maximum(value = "10000"))
        )]
        Query(page_size): Query<u64>,
        Query(search_username): Query<Option<String>>,
        Query(default_id): Query<Option<u64>>,
        #[oai(name = "X-Team-Id")] Header(team_id): Header<Option<u64>>,
        #[oai(default)] Query(name): Query<Option<String>>,
    ) -> api_response!(types::QueryWorkflowResp) {
        let search_username = if state.can_manage_job(&user_info.user_id).await? {
            search_username
        } else {
            team_id.map_or_else(|| Some(user_info.username.clone()), |_| search_username)
        };
        let svc = state.service();
        let ret = svc
            .workflow
            .get_workflow_list(
                &user_info,
                search_username.as_deref(),
                default_id,
                team_id,
                name,
                page,
                page_size,
            )
            .await?;

        let tag_records = svc
            .tag
            .get_all_tag_bind_by_resource_ids(
                ret.0.iter().map(|v| v.id).collect(),
                logic::types::ResourceType::Workflow,
            )
            .await?;

        let list = ret
            .0
            .into_iter()
            .map(|v| types::WorkflowRecord {
                id: v.id,
                name: v.name,
                info: v.info,
                tags: Some(
                    tag_records
                        .iter()
                        .filter_map(|tb| {
                            if tb.resource_id == v.id {
                                Some(types::ResourceTag {
                                    id: tb.tag_id,
                                    tag_name: tb.tag_name.clone(),
                                })
                            } else {
                                None
                            }
                        })
                        .collect(),
                ),
                team_name: v.team_name,
                team_id: v.team_id,
                updated_time: local_time!(v.updated_time),
                created_user: v.created_user,
            })
            .collect();

        return_ok!(types::QueryWorkflowResp { total: ret.1, list })
    }

    #[oai(path = "/version/list", method = "get", transform = "set_middleware")]
    pub async fn query_workflow_version(
        &self,
        state: Data<&AppState>,
        user_info: Data<&logic::types::UserInfo>,
        #[oai(default = "types::default_page", validator(maximum(value = "10000")))]
        Query(page): Query<u64>,
        #[oai(
            default = "types::default_page_size",
            validator(maximum(value = "10000"))
        )]
        Query(page_size): Query<u64>,
        Query(username): Query<Option<String>>,
        Query(workflow_id): Query<u64>,
        Query(default_id): Query<Option<u64>>,
        #[oai(name = "X-Team-Id")] Header(_team_id): Header<Option<u64>>,
        #[oai(default)] Query(name): Query<Option<String>>,
    ) -> api_response!(types::QueryWorkflowVersionResp) {
        let svc = state.service();
        let ret = svc
            .workflow
            .get_workflow_version_list(
                &user_info,
                name,
                username,
                workflow_id,
                default_id,
                page,
                page_size,
            )
            .await?;
        let list = ret
            .0
            .into_iter()
            .map::<Result<types::WorkflowVersionRecord>, _>(|v| {
                let nodes = v
                    .nodes
                    .map(|v| serde_json::from_value::<Vec<NodeConfig>>(v))
                    .transpose()?
                    .map(|v| {
                        v.into_iter()
                            .map(|v| types::NodeConfig::try_from(v))
                            .collect()
                    })
                    .transpose()?;
                let edges = v
                    .edges
                    .map(|v| serde_json::from_value::<Vec<EdgeConfig>>(v))
                    .transpose()?
                    .map(|v| {
                        v.into_iter()
                            .map(|v| types::EdgeConfig::try_from(v))
                            .collect()
                    })
                    .transpose()?;
                Ok(types::WorkflowVersionRecord {
                    id: v.id,
                    workflow_id: v.workflow_id,
                    version: v.version,
                    version_info: v.version_info,
                    nodes,
                    edges,
                    created_time: local_time!(v.created_time),
                    created_user: v.created_user,
                })
            })
            .collect::<Result<_>>()?;

        return_ok!(types::QueryWorkflowVersionResp { total: ret.1, list })
    }

    #[oai(path = "/detail", method = "get", transform = "set_middleware")]
    pub async fn get_workflow_detail(
        &self,
        state: Data<&AppState>,
        _user_info: Data<&logic::types::UserInfo>,
        Query(workflow_id): Query<u64>,
        Query(version_id): Query<Option<u64>>,
        #[oai(name = "X-Team-Id")] Header(_team_id): Header<Option<u64>>,
    ) -> api_response!(types::GetWorkflowDetailResp) {
        let svc = state.service();
        let ret = svc
            .workflow
            .get_workflow_detail(workflow_id, version_id)
            .await?;
        let nodes = ret
            .nodes
            .map(|v| serde_json::from_value::<Vec<logic::workflow::types::NodeConfig>>(v))
            .transpose()
            .context("failed convert node data")?
            .map(|v| v.into_iter().map(|v| v.try_into()).collect())
            .transpose()?;

        let edges = ret
            .edges
            .map(|v| serde_json::from_value::<Vec<logic::workflow::types::EdgeConfig>>(v))
            .transpose()
            .context("failed convert node data")?
            .map(|v| v.into_iter().map(|v| v.try_into()).collect())
            .transpose()?;

        return_ok!(types::GetWorkflowDetailResp {
            workflow_id: ret.workflow_id,
            version_id: ret.version_id,
            version: ret.version,
            version_info: ret.version_info,
            workflow_name: ret.workflow_name,
            workflow_info: ret.workflow_info,
            updated_time: local_time!(ret.updated_time),
            created_user: ret.created_user,
            nodes: nodes,
            edges: edges,
        })
    }

    #[oai(path = "/start-process", method = "post")]
    pub async fn start_process(
        &self,
        state: Data<&AppState>,
        user_info: Data<&logic::types::UserInfo>,
        Json(req): Json<types::StartProcessReq>,
        #[oai(name = "X-Team-Id")] Header(team_id): Header<Option<u64>>,
    ) -> api_response!(types::StartProcessResp) {
        let svc = state.service();
        if !svc
            .workflow
            .can_write_workflow(&user_info, team_id, Some(req.workflow_id))
            .await?
        {
            return_err!("no permission");
        }

        let process_args = if let Some(args) = req.process_args {
            Some(logic::workflow::types::WorkflowProcessArgs {
                default_target: args.default_target,
                nodes: args.nodes.map_or(None, |v| {
                    Some(
                        v.into_iter()
                            .map(|v| logic::workflow::types::WorkflowNodeArgs {
                                node_id: v.node_id,
                                target: v.target,
                                args: v.args,
                            })
                            .collect(),
                    )
                }),
            })
        } else {
            None
        };

        let process_id = svc
            .workflow
            .start_process(
                &user_info,
                req.workflow_id,
                req.version_id,
                req.process_name,
                process_args,
            )
            .await?;
        return_ok!(types::StartProcessResp { process_id })
    }
}
