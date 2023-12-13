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
    public static final String INSERT_DRC_WRITE_FILTER = "insert into drcmonitordb.drc_write_filter (`type`,`related_id`,`comment`) VALUES (%s, '%s', '%s');";
    public static final String CREATE_DRC_WRITE_FILTER = "CREATE TABLE IF NOT EXISTS `drcmonitordb`.`drc_write_filter` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `type` tinyint(4) NOT NULL,\n" +
            "  `related_id` varchar(64) NOT NULL,\n" +
            "  `comment` varchar(255) DEFAULT '',\n" +
            "  `deleted` tinyint(4) NOT NULL DEFAULT '0',\n" +
            "  `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  KEY `idx_related_id_type` (`related_id`,`type`)\n" +
            ") ENGINE=InnoDB;";

    public static final Long ONE_HOUR = 60 * 60 * 1000L;
    public static final Long TWO_HOUR = 2 * 60 * 60 * 1000L;
    public static final Long FIVE_MINUTE = 5 * 60000L;
    public static final Long ONE_DAY = 24 * 60 * 60 * 1000L;
}
