package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2023/11/30 17:26
 */
public enum DrcWriteFilterTypeEnum {
    CONFLICT(0),
    VALIDATE(1),
    ;

    private int code;

    DrcWriteFilterTypeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
