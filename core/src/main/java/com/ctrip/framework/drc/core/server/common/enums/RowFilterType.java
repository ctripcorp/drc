package com.ctrip.framework.drc.core.server.common.enums;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public enum RowFilterType {

    None(0),

    Uid(1),

    Custom(2);

    private int code;

    RowFilterType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static RowFilterType getType(int code) {
        for (RowFilterType filterType : values()) {
            if (filterType.getCode() == code) {
                return filterType;
            }
        }

        throw new UnsupportedOperationException("not support for code " + code);
    }
}
