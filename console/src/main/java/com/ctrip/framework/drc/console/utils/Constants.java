package com.ctrip.framework.drc.console.utils;

/**
 * Created by dengquanliang
 * 2023/11/14 14:41
 */
public class Constants {

    public static final String SUCCESS = "success";
    public static final String FAIL = "fail";
    public static final String BEGIN = "begin;";
    public static final String COMMIT = "commit;";
    public static final String ENDPOINT_NOT_EXIST = "endpoint not exist";
    public static final String CONFLICT_SQL_PREFIX = "/*DRC HANDLE CONFLICT*/";

    public static final String FILTER_TABLE_CREATE_SQL = "\n" +
            "CREATE TABLE `drcmonitordb`.`drc_write_filter` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',\n" +
            "  `type` tinyint(4) DEFAULT '0' COMMENT '写入类型，0-冲突处理修复写入，1-数据校验修复写入',\n" +
            "  `related_id` varchar(64) NOT NULL COMMENT '关联的操作主键id',\n" +
            "  `comment` varchar(255) DEFAULT '' COMMENT '用途说明',\n" +
            "  `deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除, 0-否; 1-是',\n" +
            " `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',\n" +
            " `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            " PRIMARY KEY (`id`)\n" +
            "  KEY `idx_related_id_type` (`related_id`,`type`)\n" +
            " ) ENGINE=InnoDB COMMENT='冲突自动处理批次表';";
}
