package com.ctrip.framework.drc.core.monitor.enums;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-10
 */
public enum DirectionEnum {

    IN(0, "in"),

    OUT(1, "out");

    private int code;

    private String description;

    DirectionEnum(int code, String description) {
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
