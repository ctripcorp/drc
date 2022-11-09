package com.ctrip.framework.drc.console.enums;

public enum ApplierTypeEnum {
    APPLIER(0,"apply to db"),
    MESSENGER(1,"apply to mq");

    private final int type;
    private final String comment;

    ApplierTypeEnum(int code, String comment) {
        this.type = code;
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public int getType() {
        return type;
    }
}
