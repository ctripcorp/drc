package com.ctrip.framework.drc.console.enums;

public enum DrcApplyModeEnum {
    MHA_APPLY(0),
    DB_APPLY(1),
    ;

    private final int code;

    DrcApplyModeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
