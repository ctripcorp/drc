package com.ctrip.framework.drc.monitor.utils.enums;

/**
 * Created by jixinwang on 2020/12/13
 */
public enum DmlTypeEnum {

    INSERT(0, "insert"),
    UPDATE(1, "update"),
    DELETE(2, "delete"),
    ACTIVE(3, "active");

    private int code;

    private String description;

    DmlTypeEnum(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
