package com.ctrip.framework.drc.core.server.common.enums;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public enum LineFilterType {

    None(0),

    Uid(1);

    private int code;

    LineFilterType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static LineFilterType getType(int code) {
        for (LineFilterType filterType : values()) {
            if (filterType.getCode() == code) {
                return filterType;
            }
        }

        throw new UnsupportedOperationException("not support for code " + code);
    }
}
