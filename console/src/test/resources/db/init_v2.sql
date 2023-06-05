CREATE DATABASE IF NOT EXISTS fxdrcmetadb;

use fxdrcmetadb;

CREATE TABLE `mha_group_tbl`
(
    `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `drc_status`               tinyint      NOT NULL DEFAULT 0 COMMENT 'DRC复制状态是否正常, 0:否; 1:是',
    `drc_establish_status`     tinyint COMMENT 'DRC搭建状态',
    `read_user`                varchar(128) COMMENT 'binlog拉取账号用户名',
    `read_password`            varchar(128) COMMENT 'binlog拉取账号密码',
    `write_user`               varchar(128) COMMENT '写账号用户名',
    `write_password`           varchar(128) COMMENT '写账号密码',
    `monitor_user`             varchar(128) COMMENT '监控账号用户名',
    `monitor_password`         varchar(128) COMMENT '监控账号密码',
    `deleted`                  tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `unit_verification_switch` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否监控, 0:否; 1:是',
    `monitor_switch`           tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否开启监控, 0:否; 1:是',
    `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='mha group表，同一个group的mha需建立DRC复制';

CREATE TABLE `region_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `region_name`         varchar(32)  NOT NULL DEFAULT '' COMMENT 'region名称',
    `deleted`             tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0-否; 1-是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_region_name` (`region_name`)
) ENGINE=InnoDB COMMENT='region表';

CREATE TABLE `mha_tbl_v2`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `mha_name`            varchar(64)  NOT NULL DEFAULT '' COMMENT 'mha名称',
    `dc_id`               bigint(20) NOT NULL DEFAULT '-1' COMMENT '机房id',
    `bu_id`               bigint(20) NOT NULL DEFAULT '-1' COMMENT 'buId',
    `cluster_name`        varchar(64)  NOT NULL DEFAULT '' COMMENT '集群名称，兼容老版',
    `read_user`           varchar(128) NOT NULL DEFAULT '' COMMENT 'binlog拉取账号用户名',
    `read_password`       varchar(128) NOT NULL DEFAULT '' COMMENT 'binlog拉取账号密码',
    `write_user`          varchar(128) NOT NULL DEFAULT '' COMMENT '写账号用户名',
    `write_password`      varchar(128) NOT NULL DEFAULT '' COMMENT '写账号密码',
    `monitor_user`        varchar(128) NOT NULL DEFAULT '' COMMENT '监控账号用户名',
    `monitor_password`    varchar(128) NOT NULL DEFAULT '' COMMENT '监控账号密码',
    `monitor_switch`      tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否开启监控, 0-否; 1-是',
    `apply_mode`          tinyint(4) NOT NULL DEFAULT '1' COMMENT 'apply mode,0-set_gtid 1-transaction_table(default)',
    `deleted`             tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0-否; 1-是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    `app_id`              bigint(11) NOT NULL DEFAULT '-1' COMMENT 'appId',
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_mha_name` (`mha_name`)
) ENGINE=InnoDB COMMENT='mha_tbl元信息表';

CREATE TABLE `mha_replication_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `src_mha_id`          bigint(20) NOT NULL DEFAULT '-1' COMMENT '源mha_id',
    `dst_mha_id`          bigint(20) NOT NULL DEFAULT '-1' COMMENT '目标端mha_id',
    `deleted`             tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0-否; 1-是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='mha同步复制链路表';

CREATE TABLE `mha_db_mapping_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `mha_id`              bigint(20) NOT NULL DEFAULT '-1' COMMENT 'mha_id',
    `db_id`               bigint(20) NOT NULL DEFAULT '-1' COMMENT 'db_id',
    `deleted`             tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0-否; 1-是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='mha和db映射表';

CREATE TABLE `db_replication_tbl`
(
    `id`                    bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `src_mha_db_mapping_id` bigint(20) NOT NULL DEFAULT '-1' COMMENT '源mha_db映射id',
    `src_logic_table_name`  varchar(255) NOT NULL DEFAULT '' COMMENT '源逻辑表',
    `dst_mha_db_mapping_id` bigint(20) NOT NULL DEFAULT '-1' COMMENT '目标端mha_db映射id',
    `dst_logic_table_name`  varchar(255) NOT NULL DEFAULT '' COMMENT '目标端逻辑表',
    `replication_type`      tinyint(4) NOT NULL DEFAULT '-1' COMMENT '复制类型 0:db-db 1-db-mq',
    `deleted`               tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0-否; 1-是',
    `create_time`           timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime`   timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY                     `idx_src_dst_mapping_id` (`src_mha_db_mapping_id`,`dst_mha_db_mapping_id`)
) ENGINE=InnoDB COMMENT='db粒度复制链路表';

CREATE TABLE `db_replication_filter_mapping_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `db_replication_id`   bigint(20) NOT NULL DEFAULT '-1' COMMENT 'db_replication_id',
    `rows_filter_id`      bigint(20) NOT NULL DEFAULT '-1' COMMENT '过滤规则id',
    `columns_filter_id`   bigint(20) NOT NULL DEFAULT '-1' COMMENT '列过滤规则id',
    `deleted`             tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0-否; 1-是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY                   `idx_db_replication_id` (`db_replication_id`)
) ENGINE=InnoDB COMMENT='过滤规则映射表';

CREATE TABLE `columns_filter_tbl_v2`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'pk',
    `mode`                tinyint(4) NOT NULL DEFAULT '-1' COMMENT '列过滤配置 模式 0-xclude 1-include',
    `columns`             text COMMENT '列',
    `deleted`             tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0-否; 1-是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='字段过滤配置表';

CREATE TABLE `applier_group_tbl_v2`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'primary key',
    `mha_replication_id`  bigint(20) NOT NULL DEFAULT '-1' COMMENT 'mha_replication_id',
    `gtid_init`           text COMMENT '初始同步位点',
    `deleted`             tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0-否; 1-是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY                   `idx_mha_replication_id` (`mha_replication_id`)
) ENGINE=InnoDB COMMENT='applier group表';

CREATE TABLE `applier_tbl_v2`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `applier_group_id`    bigint(20) NOT NULL DEFAULT '-1' COMMENT 'applier group id',
    `port`                int(11) NOT NULL DEFAULT '-1' COMMENT '端口',
    `resource_id`         bigint(20) NOT NULL DEFAULT '-1' COMMENT 'DRC资源机器id',
    `master`              tinyint(1) NOT NULL DEFAULT '-1' COMMENT '主从关系, 0:从, 1:主',
    `deleted`             tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY                   `idx_applier_group_id` (`applier_group_id`)
) ENGINE=InnoDB COMMENT='applier实例表';

CREATE TABLE `messenger_filter_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `properties`          text COMMENT 'mq配置',
    `deleted`             tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='messenger mq规则表';

CREATE TABLE `dc_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `dc_name`             varchar(30) COMMENT 'dc名称',
    `region_name`         varchar(30) COMMENT 'region名称',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `dc_name` (`dc_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dc表';

CREATE TABLE `bu_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `bu_name`             varchar(20) COMMENT 'bu名称',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `bu_name` (`bu_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='bu表';

CREATE TABLE `route_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'primary key',
    `route_org_id`        bigint(20) NOT NULL DEFAULT '0' COMMENT 'organization id of route',
    `src_dc_id`           bigint(20) NOT NULL DEFAULT '0' COMMENT 'source dc id',
    `dst_dc_id`           bigint(20) NOT NULL DEFAULT '0' COMMENT 'destination dc id',
    `src_proxy_ids`       varchar(128) NOT NULL DEFAULT '' COMMENT 'source proxies ids',
    `optional_proxy_ids`  varchar(128) NOT NULL DEFAULT '' COMMENT 'optional relay proxies',
    `dst_proxy_ids`       varchar(128) NOT NULL DEFAULT '' COMMENT 'destination proxies ids',
    `tag`                 varchar(128) NOT NULL DEFAULT '1' COMMENT 'tag for console or meta',
    `deleted`             tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'create time',
    `datachange_lasttime` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Route Info';

CREATE TABLE `proxy_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'primary key',
    `dc_id`               bigint(20) NOT NULL DEFAULT '0' COMMENT 'dc id',
    `uri`                 varchar(256) NOT NULL DEFAULT 'TCP' COMMENT 'scheme, like PROXYTCP, PROXYTLS://127.0.0.1:8080, TCP://127.0.0.1:8090',
    `active`              tinyint(4) NOT NULL DEFAULT '1' COMMENT 'active or not',
    `monitor_active`      tinyint(4) NOT NULL DEFAULT '0' COMMENT 'monitor this proxy or not',
    `deleted`             tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'create time',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT 'data changed last time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uri` (`uri`),
    KEY                   `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COMMENT='Proxy Info';

CREATE TABLE `db_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `db_name`             varchar(50) COMMENT '数据库名',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='db表';

CREATE TABLE `resource_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `app_id`              bigint(20) COMMENT '机器所在appid',
    `ip`                  varchar(16) COMMENT '机器ip',
    `dc_id`               bigint(20) COMMENT 'dc id',
    `type`                tinyint COMMENT '机器所在app应用类型',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `ip` (`ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='DRC资源机器表';

CREATE TABLE `machine_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `ip`                  varchar(16) COMMENT 'MySQL实例ip',
    `port`                int COMMENT 'MySQL实例端口',
    `uuid`                varchar(36) COMMENT 'MySQL实例uuid',
    `master`              tinyint COMMENT '主从关系, 0:从, 1:主',
    `mha_id`              bigint(20) COMMENT '集群mha id',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='MySQL机器表';

CREATE TABLE `replicator_group_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `gtid_init`           varchar(1000) COMMENT '初始gtid',
    `mha_id`              bigint(20) COMMENT '集群mha id',
    `deleted`             tinyint       NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `excluded_tables`     varchar(1024) NOT NULL DEFAULT '' COMMENT '过滤表',
    `create_time`         timestamp(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='replicator group表';

CREATE TABLE `applier_group_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `replicator_group_id` bigint(20) COMMENT 'replicator group id',
    `gtid_init`           varchar(1000) COMMENT '初始gtid',
    `mha_id`              bigint(20) COMMENT '集群mha id',
    `includedDbs`         varchar(255)          DEFAULT NULL COMMENT 'request db list, seprated by commas,',
    `name_filter`         varchar(2047)         DEFAULT NULL COMMENT 'table name filter, seprated by commas',
    `apply_mode`          tinyint      NOT NULL DEFAULT 0 COMMENT 'apply mode, 0:set gtid; 1:transaction table',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `name_mapping`        varchar(5000)         DEFAULT NULL COMMENT 'table name mapping',
    `target_name`         varchar(64)           DEFAULT NULL comment 'targetMha cluster_name,if null default use this mhas cluster_name',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='applier group表';

CREATE TABLE `cluster_manager_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `port`                int COMMENT '端口',
    `resource_id`         bigint(20) COMMENT 'DRC资源机器id',
    `master`              tinyint COMMENT '主从关系, 0:从, 1:主',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='cluster manager表';

CREATE TABLE `zookeeper_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `port`                int COMMENT '端口',
    `resource_id`         bigint(20) COMMENT 'DRC资源机器id',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='zookeeper表';

CREATE TABLE `replicator_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `port`                int COMMENT '端口',
    `applier_port`        int COMMENT '实例端口',
    `gtid_init`           varchar(1000) COMMENT '初始gtid',
    `resource_id`         bigint(20) COMMENT 'DRC资源机器id',
    `master`              tinyint COMMENT '主从关系, 0:从, 1:主',
    `relicator_group_id`  bigint(20) COMMENT 'replicator group id',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='replicator实例表';

CREATE TABLE `applier_tbl`
(
    `id`                  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `port`                int COMMENT '端口',
    `gtid_init`           varchar(1000) COMMENT '初始gtid',
    `resource_id`         bigint(20) COMMENT 'DRC资源机器id',
    `master`              tinyint COMMENT '主从关系, 0:从, 1:主',
    `applier_group_id`    bigint(20) COMMENT 'applier group id',
    `deleted`             tinyint      NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='applier实例表';

CREATE TABLE `rows_filter_tbl`
(
    id                  bigint                                                  NOT NULL AUTO_INCREMENT COMMENT 'primary key',
    name                varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL default '' COMMENT '行过滤规则名',
    mode                tinyint unsigned NOT NULL default '0' COMMENT '行过滤配置 模式 regex, trip_uid,customed',
    parameters          text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'json 保存 columns,expresssion属性',
    deleted             tinyint                                                 NOT NULL default '0' COMMENT '是否删除, 0:否; 1:是',
    create_time         timestamp(3)                                            NOT NULL default CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    datachange_lasttime timestamp(3)                                            NOT NULL default CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY name_unique (name),
    KEY                 ix_DataChange_LastTime (datachange_lasttime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci COMMENT='行过滤配置表';


