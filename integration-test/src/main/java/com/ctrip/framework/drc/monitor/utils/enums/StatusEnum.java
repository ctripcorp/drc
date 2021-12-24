package com.ctrip.framework.drc.monitor.utils.enums;

/**
 * Created by jixinwang on 2020/12/16
 */
public enum StatusEnum {
    SUCCESS(0, "success"),
    FAIL(1, "fail");


    private int code;

    private String description;

    StatusEnum(int code, String description) {
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
