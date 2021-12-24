package com.ctrip.framework.drc.console.enums;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-12
 */
public enum BooleanEnum {

    TRUE(true, 1, "true"),
    FALSE(false, 0, "false");

    private boolean value;
    private Integer code;
    private String desc;

    BooleanEnum(boolean value, Integer code, String desc) {
        this.value = value;
        this.code = code;
        this.desc = desc;
    }

    public boolean isValue() {
        return value;
    }

    public Integer getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
