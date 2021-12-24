create database if not exists fxdrcmetadb;

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
 `dc_id`                    bigint(20) COMMENT '集群mha所在dc id',
 `dns_status`               tinyint(4) NOT NULL DEFAULT '0' COMMENT 'db分机房域名是否创建, 0:否; 1:是',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`),
 UNIQUE KEY `mha_name` (`mha_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='mha表';

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
 `mha_id`                   bigint(20) COMMENT '集群mha id',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='replicator group表';

CREATE TABLE `applier_group_tbl` (
 `id`                       bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 `replicator_group_id`      bigint(20) COMMENT 'replicator group id',
 `mha_id`                   bigint(20) COMMENT '集群mha id',
 `deleted`                  tinyint NOT NULL DEFAULT 0 COMMENT '是否删除, 0:否; 1:是',
 `create_time`              timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
 `datachange_lasttime`      timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
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