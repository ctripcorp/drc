package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2023/11/13 19:57
 */
public enum SqlResultEnum {
    NOT_EXECUTED(0),
    SUCCESS(1),
    FAIL(2),
    ;

    private int code;

    SqlResultEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
