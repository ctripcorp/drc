package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2024/6/4 20:53
 */
public enum ApplierTypeEnum {
    APPLIER(1),
    DB_APPLIER(2),
    MESSENGER(3),
    DB_MESSENGER(4)
    ;
    private int code;

    ApplierTypeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
