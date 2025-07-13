use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub enum WorkflowState {
    Ready,
    Running,
    Paused,
    Completed,
    Failed(String),
}

pub struct WorkflowGraph {
    nodes: VecDeque<()>,
    edges: VecDeque<()>,
}

pub struct TaskOutput {
    pub output: String,
    pub exit_code: i64,
}

pub struct TaskError {}

pub trait Task: Send + Sync {
    fn id(&self) -> String;
    fn execute(&self, ctx: &Context) -> Result<TaskOutput, TaskError>;
}

pub struct Context {}

pub struct WorkflowExecutor<Ctx> {
    /// 工作流当前状态（运行、暂停、完成等）
    state: WorkflowState,
    /// 任务队列（按执行顺序存储）
    task_queue: VecDeque<Arc<dyn Task>>,
    /// 上下文数据（用户自定义类型，如数据库连接、配置等）
    context: Ctx,
    /// 任务依赖图（用于有向无环图 DAG 调度）
    dependency_graph: WorkflowGraph,
    /// 已完成任务结果缓存
    completed_tasks: HashMap<String, TaskOutput>,
}

pub fn start_workflow<T>(dga: &WorkflowGraph) -> WorkflowExecutor<T> {
    todo!()
}

pub struct NodeConfig {
    pub id: String,
    pub node_type: String,
}

pub fn execute_workflow<T>(node: NodeConfig) -> WorkflowExecutor<T> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
