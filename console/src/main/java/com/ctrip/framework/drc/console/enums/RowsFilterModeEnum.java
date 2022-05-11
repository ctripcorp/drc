package com.ctrip.framework.drc.console.enums;

public enum RowsFilterModeEnum {
    TRIP_UID(0,"for trip account center"),
    REGEX(1,"for regex mode"),
    CUSTOM(2,"custom mode");

    private Integer mode;
    private String comment;

    RowsFilterModeEnum(Integer mode, String comment) {
        this.mode = mode;
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public Integer getMode() {
        return mode;
    }
}
