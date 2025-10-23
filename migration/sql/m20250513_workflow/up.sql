CREATE TABLE `workflow` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
    `name` varchar(100) NOT NULL DEFAULT '' COMMENT 'workflow name',
    `nodes` json DEFAULT NULL COMMENT 'workflow nodes',
    `edges` json DEFAULT NULL COMMENT 'workflow edges',
    `info` varchar(500) NOT NULL DEFAULT '' COMMENT 'describe message',
    `team_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT 'team id',
    `created_user` varchar(50) NOT NULL DEFAULT '' COMMENT 'creator username',
    `updated_user` varchar(50) NOT NULL DEFAULT '' COMMENT 'updater username',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'updated time',
    `is_deleted` BOOLEAN NOT NULL DEFAULT false COMMENT 'is deleted',
    `deleted_at` timestamp NULL DEFAULT NULL COMMENT 'deleted time',
    `deleted_by` varchar(50) NOT NULL DEFAULT '' COMMENT 'deleted by',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'workflow';

CREATE TABLE `workflow_version` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
    `workflow_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT 'parent id',
    `version` VARCHAR(100) NOT NULL DEFAULT '' COMMENT 'version',
    `version_info` varchar(500) NOT NULL DEFAULT '' COMMENT 'describe message',
    `nodes` json DEFAULT NULL COMMENT 'workflow nodes',
    `edges` json DEFAULT NULL COMMENT 'workflow edges',
    `team_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT 'team id',
    `created_user` varchar(50) NOT NULL DEFAULT '' COMMENT 'creator username',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
    `is_deleted` BOOLEAN NOT NULL DEFAULT false COMMENT 'is deleted',
    `deleted_at` timestamp NULL DEFAULT NULL COMMENT 'deleted time',
    `deleted_by` varchar(50) NOT NULL DEFAULT '' COMMENT 'deleted by',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'workflow version';

CREATE TABLE `workflow_process` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
    `process_id` VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'process id',
    `process_name` varchar(100) NOT NULL DEFAULT '' COMMENT 'process name',
    `workflow_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT 'workflow id',
    `version_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT 'version id',
    `process_status` varchar(100) NOT NULL DEFAULT 'start_process' COMMENT 'process status',
    `current_node_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'current node id',
    `current_node_status` varchar(100) NOT NULL DEFAULT '' COMMENT 'current node status',
    `current_run_id` VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'current run id',
    `created_user` varchar(50) NOT NULL DEFAULT '' COMMENT 'creator username',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'updated time',
    `process_args` json DEFAULT NULL COMMENT 'process args',
    `is_deleted` BOOLEAN NOT NULL DEFAULT false COMMENT 'is deleted',
    `deleted_at` timestamp NULL DEFAULT NULL COMMENT 'deleted time',
    `deleted_by` varchar(50) NOT NULL DEFAULT '' COMMENT 'deleted by',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB AUTO_INCREMENT = 23 DEFAULT CHARSET = utf8mb4 COMMENT = 'workflow process';

CREATE TABLE `workflow_process_node` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
    `process_id` VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'process id',
    `run_id` VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'run id',
    `node_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'process node id',
    `node_status` varchar(100) NOT NULL DEFAULT 'start' COMMENT 'node status:start running, end',
    `node_args` json DEFAULT NULL COMMENT 'node args',
    `depth` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'workflow node depth',
    `created_user` varchar(50) NOT NULL DEFAULT '' COMMENT 'creator username',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'updated time',
    PRIMARY KEY (`id`),
    KEY `idx_process_id` (`process_id`, `node_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'workflow process node';

CREATE TABLE `workflow_process_node_task` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
    `process_id` VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'process id',
    `run_id` VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'run id',
    `node_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'process node id',
    `task_status` varchar(100) NOT NULL DEFAULT 'start' COMMENT 'node status:start running, end',
    `bind_instance_id` VARCHAR(40) NOT NULL DEFAULT '' COMMENT 'node bind instance id',
    `bind_ip` char(20) NOT NULL DEFAULT '' COMMENT 'node bind ip',
    `exit_code` tinyint NOT NULL DEFAULT '0' COMMENT 'exit code',
    `exit_status` varchar(200) NOT NULL DEFAULT '' COMMENT 'exit status',
    `output` text NOT NULL COMMENT 'execute output',
    `restart_num` int NOT NULL DEFAULT '0' COMMENT 'restart number',
    `dispatch_result` json DEFAULT NULL COMMENT 'dispatch result',
    `created_user` varchar(50) NOT NULL DEFAULT '' COMMENT 'creator username',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'updated time',
    PRIMARY KEY (`id`),
    KEY `idx_process_id` (`process_id`, `node_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'workflow process node task';

CREATE TABLE `workflow_process_edge` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
    `process_id` VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'process id',
    `run_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'run id',
    `edge_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'edge id',
    `edge_type` varchar(50) NOT NULL DEFAULT '' COMMENT 'edge type',
    `eval_val` varchar(100) NOT NULL DEFAULT '' COMMENT 'eval val',
    `props` json DEFAULT NULL COMMENT 'properties',
    `source_node_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'source node id',
    `target_node_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'target node id',
    `created_user` varchar(50) NOT NULL DEFAULT '' COMMENT 'creator username',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
    PRIMARY KEY (`id`),
    KEY `idx_process_id` (`process_id`),
    KEY `idx_source_node_id` (`source_node_id`),
    KEY `idx_target_node_id` (`target_node_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '任务进程中连接任务的边线';

ALTER TABLE
    job_schedule_history
ADD
    COLUMN `actual_args` json DEFAULT NULL COMMENT 'arguments';

ALTER TABLE
    job_timer
ADD
    COLUMN `job_args` json DEFAULT NULL COMMENT '作业参数';

ALTER TABLE
    job_supervisor
ADD
    COLUMN `job_args` json DEFAULT NULL COMMENT '作业参数';

CREATE TABLE `workflow_timer` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
    `workflow_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'workflow id',
    `version_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'workflow version id',
    `name` varchar(100) NOT NULL DEFAULT '' COMMENT 'workflow name',
    `timer_expr` JSON NULL COMMENT 'timer expr',
    `schedule_guid` varchar(100) NOT NULL DEFAULT '' COMMENT 'scheduler schedule_guid',
    `is_active` BOOLEAN NOT NULL DEFAULT false COMMENT 'is timer active',
    `info` VARCHAR(500) NOT NULL DEFAULT '' COMMENT 'timer info',
    `process_args` JSON NULL COMMENT 'workflow process args',
    `created_user` varchar(50) NOT NULL DEFAULT '' COMMENT 'creator username',
    `updated_user` varchar(50) NOT NULL DEFAULT '' COMMENT 'updater username',
    `next_time` timestamp NULL DEFAULT NULL COMMENT 'next execution time',
    `prev_time` timestamp NULL DEFAULT NULL COMMENT 'prev execution time',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created time',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'updated time',
    `is_deleted` BOOLEAN NOT NULL DEFAULT false COMMENT 'is deleted',
    `deleted_at` timestamp NULL DEFAULT NULL COMMENT 'deleted time',
    `deleted_by` varchar(50) NOT NULL DEFAULT '' COMMENT 'deleted by',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'workflow timer';