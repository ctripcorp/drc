package com.ctrip.framework.drc.console.enums.v2;

/**
 * Created by dengquanliang
 * 2024/1/31 16:57
 */
public enum MhaReplicationTypeEnum {
    SINGLE(0),
    DOUBLE(1)
    ;

    private int code;

    MhaReplicationTypeEnum(int code) {
        this.code = code;
    }
}
