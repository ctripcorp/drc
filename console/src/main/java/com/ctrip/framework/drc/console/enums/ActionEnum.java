package com.ctrip.framework.drc.console.enums;

/**
 * @Author: hbshen
 * @Date: 2021/4/25
 */
public enum ActionEnum {

    ADD(0),
    UPDATE(1),
    DELETE(2);

    private int code;

    ActionEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
