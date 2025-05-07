package com.ctrip.framework.drc.console.enums.error;

import com.ctrip.framework.drc.console.enums.IErrorDef;

public enum AutoBuildErrorEnum implements IErrorDef {
    DRC_SAME_REGION_NOT_SUPPORTED("DRC_SAME_REGION_NOT_SUPPORTED", "drc between same region is not supported yet!"),
    DRC_SAME_MHA_NOT_SUPPORTED("DRC_MHA_NOT_SUPPORTED", "drc between same mha is not supported yet!"),
    DRC_NO_OR_MULTI_MHA_OPTIONS_NOT_SUPPORTED("DRC_NO_OR_MULTI_MHA_OPTIONS_NOT_SUPPORTED", "same region but zero/multi mha options, not supported yet!"),
    GTID_PURGED("GTID_PURGED", "given gtid is purged, cannot config replicator!"),
    GTID_ONLY_FOR_SINGLE_MHA_REPLICATION("GTID_ONLY_FOR_SINGLE_MHA_REPLICATION", "gtid is allowed to config if only [1] mha replication is related"),
    GTID_NOT_CONFIGURABLE_FOR_EXIST_REPLICATION("GTID_NOT_CONFIGURABLE_FOR_EXIST_REPLICATION", "should not configure gtid for exist replication!"),
    DB_APPLIERS_NOT_CONSISTENT("DB_APPLIERS_NOT_CONSISTENT", "exist db replication applier configuration not consistent, could not auto-build, please manually check and build!"),
    DB_REPLICATION_NOT_CONSISTENT("DB_REPLICATION_NOT_CONSISTENT", "exist db replication configuration not consistent, please manually check and fix."),
    ORIGINAL_DB_REPLICATION_CONFIG_NOT_EXIST("ORIGINAL_DB_REPLICATION_CONFIG_NOT_EXIST", "original configuration is missing. maybe it's modified or deleted. please fresh the page and try again"),
    CREATE_MHA_DB_REPLICATION_FAIL_CONFIG_EXIST("CREATE_MHA_DB_REPLICATION_FAIL_CONFIG_EXIST", "replication already exists. please fresh the page and try again"),
    GET_BU_CODE_FOR_DB_FAIL("GET_BU_CODE_FOR_DB_FAIL", "get bu code fail. different bu code found for dbs"),
    PRE_CHECK_MYSQL_TABLE_INFO_FAIL("PRE_CHECK_MYSQL_TABLE_INFO_FAIL", "pre check mysql table fail."),

    /**
     * mq
     */
    MQ_CONFIG_CHECK_FAIL("MQ_CONFIG_CHECK_FAIL", "pre check mq config fail"),
    CONFIGURE_MESSENGER_MHA_FAIL("CONFIGURE_MESSENGER_MHA_FAIL", "configure messenger mha fail"),
    DUPLICATE_MQ_CONFIGURATION("DUPLICATE_MQ_CONFIGURATION", "duplicate mq table-topic configuration found"),
    ;


    AutoBuildErrorEnum(String code, String message) {
        this.code = code;
        this.message = message;
    }

    private final String code;
    private final String message;

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
