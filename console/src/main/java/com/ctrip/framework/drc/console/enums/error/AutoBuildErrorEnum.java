package com.ctrip.framework.drc.console.enums.error;

import com.ctrip.framework.drc.console.enums.IErrorDef;

public enum AutoBuildErrorEnum implements IErrorDef {
    DRC_SAME_REGION_NOT_SUPPORTED("DRC_SAME_REGION_NOT_SUPPORTED", "drc between same region is not supported yet!"),
    DRC_MULTI_MHA_OPTIONS_IN_SAME_REGION_NOT_SUPPORTED("DRC_MULTI_MHA_OPTIONS_IN_SAME_REGION_NOT_SUPPORTED", "same region but multi mha options, not supported yet!"),
    ;


    AutoBuildErrorEnum(String code, String message) {
        this.code = code;
        this.message = message;
    }

    private final String code;
    private final String message;

    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getMessage() {
        return null;
    }
}
