package com.ctrip.framework.drc.core.mq;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MONITOR_SCHEMA_NAME;

/**
 * @author yongnian
 * @create 2025/1/10 15:42
 */
public class MessengerGtidTbl {
    public static final String TABLE_NAME = "messenger_gtid_executed";
    public static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS `drcmonitordb`.`messenger_gtid_executed`\n" +
            "(\n" +
            "    `id`                  bigint(20)   NOT NULL AUTO_INCREMENT,\n" +
            "    `registry_key`        varchar(512) NOT NULL,\n" +
            "    `gtid_set`            longtext,\n" +
            "    `create_time`         timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n" +
            "    `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "    PRIMARY KEY (`id`),\n" +
            "    UNIQUE KEY `idx_registry_key` (`registry_key`)\n" +
            ") ENGINE = InnoDB\n" +
            "  DEFAULT CHARSET = utf8mb4;";

    public static Map<String, Map<String, String>> getDDLSchemas() {
        Map<String, Map<String, String>> dbMap = new HashMap<>();
        Map<String, String> tableMap = new HashMap<>();
        dbMap.put(DRC_MONITOR_SCHEMA_NAME, tableMap);
        tableMap.put(MessengerGtidTbl.TABLE_NAME, MessengerGtidTbl.CREATE_TABLE_SQL);
        return Collections.unmodifiableMap(dbMap);
    }

}
