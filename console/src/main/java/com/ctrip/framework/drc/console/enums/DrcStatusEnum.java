package com.ctrip.framework.drc.console.enums;

public enum DrcStatusEnum {
    NOT_EXIST(-1),
    STOP(0),
    STARTED(1),
    PARTIAL_STARTED(2),
    ;

    private final int code;

    DrcStatusEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
