package com.ctrip.framework.drc.core.monitor.enums;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-02-05
 */
public enum AutoIncrementEnum {

    CORRECT(0L, "correct auto increment configuration"),

    INCORRECT(1L, "incorrect auto increment configuration");

    private Long code;

    private String description;

    AutoIncrementEnum(Long code, String description) {
        this.code = code;
        this.description = description;
    }

    public Long getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
