package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2023/11/6 18:16
 */
public enum ApprovalTypeEnum {
    DB_OWNER(0),
    DBA(1),
    ;

    private int code;

    ApprovalTypeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
