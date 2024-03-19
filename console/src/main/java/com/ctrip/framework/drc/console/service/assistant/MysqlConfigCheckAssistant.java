package com.ctrip.framework.drc.console.service.assistant;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2024/2/20 16:04
 */
public class MysqlConfigCheckAssistant {

    public static final String BINLOG_MODE_KEY = "binlogMode";
    public static final String BINLOG_MODE_VALUE = "ON";

    public static final String BINLOG_FORMAT_KEY = "binlogFormat";
    public static final String BINLOG_FORMAT_VALUE = "ROW";

    public static final String BINLOG_VERSION_KEY = "binlogVersion1";
    public static final String BINLOG_VERSION_VALUE = "OFF";

    public static final String BINLOG_TRANSACTION_DEPENDENCY_KEY = "binlogTransactionDependency";
    public static final String BINLOG_TRANSACTION_DEPENDENCY_VALUE = "WRITESET";

    public static final String BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_KEY = "binlogTransactionDependencyHistorySize";
    public static final int BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_VALUE = 100000;

    public static final String GTID_MODE_KEY = "gtidMode";
    public static final String GTID_MODE_VALUE = "ON";

    public static final String DRC_TABLES_KEY = "drcTables";
    public static final int DRC_TABLES_VALUE = 2;

    public static final String AUTO_INCREMENT_STEP_KEY = "autoIncrementStep";

    public static final String AUTO_INCREMENT_OFFSET_KEY = "autoIncrementOffset";

    public static final String BINLOG_ROW_IMAGE_KEY = "binlogRowImage";
    public static final String BINLOG_ROW_IMAGE_VALUE = "FULL";

    public static final String DRC_ACCOUNT_KEY = "drcAccounts";
    public static final String DRC_ACCOUNT_VALUE = "three accounts ready";

    public static boolean checkMysqlConfig(Map<String, Object> configMap) {
        if (!BINLOG_MODE_VALUE.equalsIgnoreCase(String.valueOf(configMap.get(BINLOG_MODE_KEY)))) {
            return false;
        }
        if (!BINLOG_FORMAT_VALUE.equalsIgnoreCase(String.valueOf(configMap.get(BINLOG_FORMAT_KEY)))) {
            return false;
        }
        if (!BINLOG_VERSION_VALUE.equalsIgnoreCase(String.valueOf(configMap.get(BINLOG_VERSION_KEY)))) {
            return false;
        }
        if (!BINLOG_TRANSACTION_DEPENDENCY_VALUE.equalsIgnoreCase(String.valueOf(configMap.get(BINLOG_TRANSACTION_DEPENDENCY_KEY)))) {
            return false;
        }
        if (BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_VALUE != (Integer) configMap.get(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_KEY)) {
            return false;
        }
        if (!GTID_MODE_VALUE.equalsIgnoreCase(String.valueOf(configMap.get(GTID_MODE_KEY)))) {
            return false;
        }
        if (DRC_TABLES_VALUE > (Integer) configMap.get(DRC_TABLES_KEY)) {
            return false;
        }
        if (!BINLOG_ROW_IMAGE_VALUE.equalsIgnoreCase(String.valueOf(configMap.get(BINLOG_ROW_IMAGE_KEY)))) {
            return false;
        }
        if (!DRC_ACCOUNT_VALUE.equalsIgnoreCase(String.valueOf(configMap.get(DRC_ACCOUNT_KEY)))) {
            return false;
        }
        return true;
    }


}
