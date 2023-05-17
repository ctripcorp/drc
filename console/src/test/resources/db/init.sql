-- 延时监控表
CREATE DATABASE IF NOT EXISTS drcmonitordb;

CREATE TABLE `drcmonitordb`.`delaymonitor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `src_ip` varchar(15) NOT NULL,
  `dest_ip` varchar(15) NOT NULL,
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE DATABASE IF NOT EXISTS fxdrcmetadb;

CREATE TABLE `fxdrcmetadb`.`applier_upload_log_tbl` (
  `log_id`              BIGINT NOT NULL AUTO_INCREMENT,
  `src_dc_name`         VARCHAR(30),
  `dest_dc_name`        VARCHAR(30),
  `cluster_name`        VARCHAR(30),
  `uid`                 VARCHAR(127),
  `log_type`            VARCHAR(20),
  `sql_statement`       VARCHAR(15000),
  `sql_time`            BIGINT,
  `datachange_lasttime` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`log_id`)
)engine=INNODB DEFAULT CHARSET=utf8 COMMENT='applier_upload_log_tbl';

use fxdrcmetadb;

CREATE TABLE `dc_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `dc_name`                  varchar(30) COMMENT 'dc名称',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`),
 UNIQUE KEY `dc_name` (`dc_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dc表';

CREATE TABLE `bu_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `bu_name`                  varchar(20) COMMENT 'bu名称',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`),
 UNIQUE KEY `bu_name` (`bu_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='bu表';

CREATE TABLE `cluster_tbl`
(
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `cluster_name`             varchar(64) COMMENT 'dal cluster名称',
 `cluster_app_id`           bigint(20) COMMENT '业务应用appid',
 `bu_id`                    bigint(20) COMMENT 'cluster所在BU',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (id),
  UNIQUE KEY `cluster_name` (`cluster_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='cluster表';

CREATE TABLE `mha_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `mha_name`                 varchar(50) COMMENT '集群mha名称',
 `mha_group_id`             bigint(20) COMMENT '集群mha group id，表示复制关系',
 `monitor_switch` tinyint(4) not null default '0'  comment '是否开启监控, 0:否; 1:是',
 `dc_id`                    bigint(20) COMMENT '集群mha所在dc id',
 `dns_status`               tinyint(4) NOT NULL DEFAULT '0' COMMENT 'db分机房域名是否创建, 0:否; 1:是',
 `apply_mode`               tinyint NOT NULL DEFAULT 1 COMMENT 'apply mode, 0:set gtid; 1:transaction table',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`),
 UNIQUE KEY `mha_name` (`mha_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='mha表';

CREATE TABLE `route_tbl` (
	`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'primary key',
	`route_org_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'organization id of route',
	`src_dc_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'source dc id',
	`dst_dc_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'destination dc id',
	`src_proxy_ids` varchar(128) NOT NULL DEFAULT '' COMMENT 'source proxies ids',
    `optional_proxy_ids` varchar(128) NOT NULL DEFAULT '' COMMENT 'optional relay proxies',
	`dst_proxy_ids` varchar(128) NOT NULL DEFAULT '' COMMENT 'destination proxies ids',
	`tag` varchar(128) NOT NULL DEFAULT '1' COMMENT 'tag for console or meta',
	`deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
	`create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'create time',
	`datachange_lasttime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'data changed last time',
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Route Info';

CREATE TABLE `proxy_tbl` (
     `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'primary key',
     `dc_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'dc id',
     `uri` varchar(256) NOT NULL DEFAULT 'TCP' COMMENT 'scheme, like PROXYTCP, PROXYTLS://127.0.0.1:8080, TCP://127.0.0.1:8090',
     `active` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'active or not',
     `monitor_active` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'monitor this proxy or not',
     `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'deleted or not',
     `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'create time',
     `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'data changed last time',
     PRIMARY KEY (`id`),
     UNIQUE KEY `uri` (`uri`),
     KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COMMENT='Proxy Info';


CREATE TABLE `cluster_mha_map_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `cluster_id`               bigint(20) COMMENT 'dal cluster id',
 `mha_id`                   bigint(20) COMMENT 'mha id',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`),
 UNIQUE KEY `cmkey` (`cluster_id`,`mha_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='cluster mha映射表';

CREATE TABLE `group_mapping_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `mha_group_id`             bigint(20) DEFAULT NULL COMMENT 'mha group id',
 `mha_id`                   bigint(20) DEFAULT NULL COMMENT 'mha id',
 `deleted`                  tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`),
 KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='mha group与mha映射表';

CREATE TABLE `mha_group_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `drc_status`               tinyint NOT NULL DEFAULT 0 COMMENT 'DRC复制状态是否正常, 0:否; 1:是',
 `drc_establish_status`     tinyint COMMENT 'DRC搭建状态',
 `read_user`                varchar(128) COMMENT 'binlog拉取账号用户名',
 `read_password`            varchar(128) COMMENT 'binlog拉取账号密码',
 `write_user`               varchar(128) COMMENT '写账号用户名',
 `write_password`           varchar(128) COMMENT '写账号密码',
 `monitor_user`             varchar(128) COMMENT '监控账号用户名',
 `monitor_password`         varchar(128) COMMENT '监控账号密码',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `unit_verification_switch` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否监控, 0:否; 1:是',
 `monitor_switch`           tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否开启监控, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='mha group表，同一个group的mha需建立DRC复制';

CREATE TABLE `db_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `db_name`                  varchar(50) COMMENT '数据库名',
 `mha_group_id`             bigint(20) COMMENT '集群mha group id，表示复制关系',
 `dns_status`               tinyint NOT NULL DEFAULT 0 COMMENT 'db分机房域名是否创建, 0:否; 1:是',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='db表';

CREATE TABLE `resource_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `app_id`                   bigint(20) COMMENT '机器所在appid',
 `ip`                       varchar(16) COMMENT '机器ip',
 `dc_id`                    bigint(20) COMMENT 'dc id',
 `type`                     tinyint COMMENT '机器所在app应用类型',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`),
 UNIQUE KEY `ip` (`ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='DRC资源机器表';

CREATE TABLE `machine_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `ip`                       varchar(16) COMMENT 'MySQL实例ip',
 `port`                     int COMMENT 'MySQL实例端口',
 `uuid`                     varchar(36) COMMENT 'MySQL实例uuid',
 `master`                   tinyint COMMENT '主从关系, 0:从, 1:主',
 `mha_id`                   bigint(20) COMMENT '集群mha id',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='MySQL机器表';

CREATE TABLE `replicator_group_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `gtid_init`                varchar(1000) COMMENT '初始gtid',
 `mha_id`                   bigint(20) COMMENT '集群mha id',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `excluded_tables`          varchar(1024) NOT NULL DEFAULT '' COMMENT '过滤表',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='replicator group表';

CREATE TABLE `applier_group_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `replicator_group_id`      bigint(20) COMMENT 'replicator group id',
 `gtid_init`                varchar(1000) COMMENT '初始gtid',
 `mha_id`                   bigint(20) COMMENT '集群mha id',
 `includedDbs`              varchar(255) DEFAULT NULL COMMENT 'request db list, seprated by commas,',
 `name_filter`              varchar(2047) DEFAULT NULL COMMENT 'table name filter, seprated by commas',
 `apply_mode`               tinyint NOT NULL DEFAULT 0 COMMENT 'apply mode, 0:set gtid; 1:transaction table',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `name_mapping`             varchar(5000) DEFAULT NULL COMMENT 'table name mapping',
 `target_name`               varchar(64)  DEFAULT NULL   comment 'targetMha cluster_name,if null default use this mhas cluster_name',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  `gtid_executed`           varchar(2000)  NULL default null COMMENT 'applier 起始位点，可以替代qconfig 配置',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='applier group表';

CREATE TABLE `cluster_manager_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `port`                     int COMMENT '端口',
 `resource_id`              bigint(20) COMMENT 'DRC资源机器id',
 `master`                   tinyint COMMENT '主从关系, 0:从, 1:主',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='cluster manager表';

CREATE TABLE `zookeeper_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `port`                     int COMMENT '端口',
 `resource_id`              bigint(20) COMMENT 'DRC资源机器id',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='zookeeper表';

CREATE TABLE `replicator_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `port`                     int COMMENT '端口',
 `applier_port`             int COMMENT '实例端口',
 `gtid_init`                varchar(1000) COMMENT '初始gtid',
 `resource_id`              bigint(20) COMMENT 'DRC资源机器id',
 `master`                   tinyint COMMENT '主从关系, 0:从, 1:主',
 `relicator_group_id`       bigint(20) COMMENT 'replicator group id',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='replicator实例表';

CREATE TABLE `applier_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `port`                     int COMMENT '端口',
 `gtid_init`                varchar(1000) COMMENT '初始gtid',
 `resource_id`              bigint(20) COMMENT 'DRC资源机器id',
 `master`                   tinyint COMMENT '主从关系, 0:从, 1:主',
 `applier_group_id`         bigint(20) COMMENT 'applier group id',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='applier实例表';

CREATE TABLE ddl_history_tbl
(
  id                    BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
  mha_id                BIGINT COMMENT 'mha id',
  ddl                   VARCHAR(1024) NOT NULL DEFAULT '' COMMENT 'ddl信息',
  schema_name           VARCHAR(50) NOT NULL DEFAULT '' COMMENT '库名',
  table_name            VARCHAR(50) NOT NULL DEFAULT '' COMMENT '表名',
  query_type            TINYINT NOT NULL DEFAULT -1 COMMENT 'ddl类型',
  create_time           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  datachange_lasttime   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ddl_history表';

CREATE TABLE `data_consistency_monitor_tbl` (
 `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `mha_id` int(11) NOT NULL DEFAULT '0' COMMENT '需要监控的mha_id',
 `monitor_schema_name` varchar(255) NOT NULL DEFAULT '' COMMENT '需要监控的数据库名',
 `monitor_table_name` varchar(255) NOT NULL DEFAULT '' COMMENT '需要监控的表名',
 `monitor_table_key` varchar(255) NOT NULL DEFAULT '' COMMENT '需要监控的表key',
 `monitor_table_on_update` varchar(255) NOT NULL DEFAULT '' COMMENT '需要监控的表onUpdate字段',
 `monitor_switch` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否监控, 0:否; 1:是',
 `full_data_check_status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '全量数据一致性校验状态，0:未校验；1:校验中；2:校验完',
 `full_data_check_lasttime` timestamp(3) NULL DEFAULT NULL COMMENT '最近全量数据一致性校验时间',
 `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据一致性监控配置表';

CREATE TABLE `data_inconsistency_history_tbl` (
 `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `monitor_schema_name` varchar(255) NOT NULL DEFAULT '' COMMENT '监控的库名',
 `monitor_table_name` varchar(255) NOT NULL DEFAULT '' COMMENT '监控的表名',
 `monitor_table_key` varchar(255) NOT NULL DEFAULT '' COMMENT '监控的表key',
 `monitor_table_key_value` varchar(255) NOT NULL DEFAULT '' COMMENT '监控表不一致数值对应key的值',
 `mha_group_id` bigint(20) DEFAULT NULL COMMENT '集群mha group id',
 `source_type` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '存储来源,1:增量; 2:全量',
 `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`),   KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
 ) ENGINE=InnoDB AUTO_INCREMENT=27385 DEFAULT CHARSET=utf8 COMMENT='数据不一致历史表';

CREATE TABLE `unit_route_verification_history_tbl` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
`gtid` varchar(64) NOT NULL DEFAULT '' COMMENT '事务id',
`query_sql` varchar(1000) NOT NULL DEFAULT '' COMMENT '执行SQL语句',
`expected_dc` varchar(30) NOT NULL DEFAULT '' COMMENT '路由策略机房',
`actual_dc` varchar(30) NOT NULL DEFAULT '' COMMENT '实际写入机房',
`columns` varchar(1000) NOT NULL DEFAULT '' COMMENT '表字段列表',
`before_values` varchar(1000) NOT NULL DEFAULT '' COMMENT '影响前内容',
`after_values` varchar(1000) NOT NULL DEFAULT '' COMMENT '影响后内容',
`uid_name` varchar(30) NOT NULL DEFAULT '' COMMENT '用户id字段名',
`ucs_strategy_id` int(11) NOT NULL DEFAULT '-1' COMMENT 'ucs策略号',
`mha_group_id` bigint(20) DEFAULT NULL COMMENT '集群mha group id',
`create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
`schema_name` varchar(255) NOT NULL DEFAULT '' COMMENT '库名',
`table_name` varchar(255) NOT NULL DEFAULT '' COMMENT '表名',
`execute_time` timestamp(3) NULL DEFAULT NULL COMMENT '执行时间',
PRIMARY KEY (`id`),
KEY `mha_group_id` (`mha_group_id`),
KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB AUTO_INCREMENT=9127 DEFAULT CHARSET=utf8 COMMENT='单元化路由校验不一致表';

CREATE TABLE `rows_filter_mapping_tbl`
(
    id                  bigint       NOT NULL AUTO_INCREMENT COMMENT 'pk',
    data_media_id       bigint       NOT NULL default '-1' COMMENT 'data_media_index',
    rows_filter_id      bigint       NOT NULL default '-1' COMMENT 'rows_filter_index',
    deleted             tinyint      NOT NULL default '0' COMMENT '是否删除, 0:否; 1:是',
    create_time         timestamp(3) NOT NULL default CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    datachange_lasttime timestamp(3) NOT NULL default CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    applier_group_id    bigint       NOT NULL default '-1' COMMENT 'applier_group_id_index',
    PRIMARY KEY (id),
    UNIQUE KEY unique_mapping (applier_group_id,data_media_id,rows_filter_id),
    KEY                 rows_filter_index (rows_filter_id),
    KEY                 ix_DataChange_LastTime (datachange_lasttime),
    KEY                 data_media_index (data_media_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='rows_filter 和 data_media 映射表';

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

CREATE TABLE `data_media_tbl`
(
    id                   bigint                                                        NOT NULL AUTO_INCREMENT COMMENT 'pk',
    namespcae            varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL default '' COMMENT 'schema,topic等，逻辑概念',
    name                 varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL default '' COMMENT '表名，逻辑概念',
    type                 tinyint unsigned NOT NULL default '0' COMMENT '0 正则逻辑表，1 映射逻辑表',
    data_media_source_id bigint                                                        NOT NULL default '-1' COMMENT 'mysql 使用mha_id,索引列',
    deleted              tinyint                                                       NOT NULL default '0' COMMENT '是否删除, 0:否; 1:是',
    create_time          timestamp(3)                                                  NOT NULL default CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    datachange_lasttime  timestamp(3)                                                  NOT NULL default CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP (3) COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY data_media_name_unique (namespcae, name),
    KEY                  source_id_index (data_media_source_id),
    KEY                  ix_DataChange_LastTime (datachange_lasttime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='data_media 逻辑表';
