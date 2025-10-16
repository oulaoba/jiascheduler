DROP TABLE IF EXISTS `workflow`;

DROP TABLE IF EXISTS `workflow_version`;

DROP TABLE IF EXISTS `workflow_process`;

DROP TABLE IF EXISTS `workflow_process_node`;

DROP TABLE IF EXISTS `workflow_process_node_task`;

DROP TABLE IF EXISTS `workflow_process_edge`;

ALTER TABLE
    job_schedule_history DROP COLUMN `actual_args`;

ALTER TABLE
    job_timer DROP COLUMN `job_args`;

ALTER TABLE
    job_supervisor DROP COLUMN `job_args`;

alter table
    workflow_process drop column is_deleted,
    drop column deleted_at,
    drop column deleted_by;

DROP TABLE IF EXISTS `workflow_timer`;