package com.ctrip.framework.drc.core.monitor.enums;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-30
 */
public enum ConsistencyEnum {

    CONSISTENT(0L, "consistent"),

    NON_CONSISTENT(1L, "not consistent");

    private Long code;

    private String description;

    ConsistencyEnum(Long code, String description) {
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
