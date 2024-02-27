package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2023/11/6 19:33
 */
public enum ApprovalResultEnum {
    UNDER_APPROVAL(0),
    APPROVED(1),
    REJECTED(2),
    NOT_APPROVED(3),
    CLOSED(4)
    ;

    private int code;

    ApprovalResultEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
