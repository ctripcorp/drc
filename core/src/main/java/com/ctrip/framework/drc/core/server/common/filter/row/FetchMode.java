package com.ctrip.framework.drc.core.server.common.filter.row;

/**
 * @Author limingdong
 * @create 2022/6/30
 */
public enum FetchMode {

    RPC(0),

    BlackList(1),

    WhiteList(2);

    private int code;

    FetchMode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static FetchMode getMode(int code) {
        for (FetchMode fetchMode : values()) {
            if (fetchMode.getCode() == code) {
                return fetchMode;
            }
        }

        throw new UnsupportedOperationException("not support for code " + code);
    }
}
