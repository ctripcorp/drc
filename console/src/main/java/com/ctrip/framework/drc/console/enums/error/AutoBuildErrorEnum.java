package com.ctrip.framework.drc.console.enums.error;

import com.ctrip.framework.drc.console.enums.IErrorDef;

public enum AutoBuildErrorEnum implements IErrorDef {
    DRC_SAME_REGION_NOT_SUPPORTED("DRC_SAME_REGION_NOT_SUPPORTED", "drc between same region is not supported yet!"),
    DRC_SAME_MHA_NOT_SUPPORTED("DRC_MHA_NOT_SUPPORTED", "drc between same mha is not supported yet!"),
    DRC_NO_OR_MULTI_MHA_OPTIONS_NOT_SUPPORTED("DRC_NO_OR_MULTI_MHA_OPTIONS_NOT_SUPPORTED", "same region but zero/multi mha options, not supported yet!"),
    GTID_PURGED("GTID_PURGED", "given gtid is purged, cannot config replicator!"),
    GTID_ONLY_FOR_SINGLE_MHA_REPLICATION("GTID_ONLY_FOR_SINGLE_MHA_REPLICATION", "gtid is allowed to config if only [1] mha replication is related"),
    GTID_NOT_CONFIGURABLE_FOR_EXIST_REPLICATION("GTID_NOT_CONFIGURABLE_FOR_EXIST_REPLICATION", "should not configure gtid for exist replication!"),
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
