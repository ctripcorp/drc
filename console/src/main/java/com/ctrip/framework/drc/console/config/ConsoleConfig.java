package com.ctrip.framework.drc.console.config;

public class ConsoleConfig {
    public static final String GTID_EXECUTED_COMMAND = "show master status;";
    public static final String UUID_COMMAND = "/*FORCE_MASTER*/show global variables like 'server_uuid';";
    public static final int ZERO_ROWS_AFFECT = 0;
    public static final int SHOULD_AFFECTED_ROWS = 2;
    public static final int UUID_INDEX = 2;
    public static final int MHA_GROUP_SIZE = 2;
    public static final int GTID_EXECUTED_INDEX = 5;
    public static final int DEFAULT_REPLICATOR_PORT = 8080;
    public static final int DEFAULT_REPLICATOR_APPLIER_PORT = 8383;
    public static final int DEFAULT_APPLIER_PORT = 8080;
    public static final int ADD_REMOVE_PAIR_SIZE = 2;
}
