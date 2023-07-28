package com.ctrip.framework.drc.console.enums;

/**
 * Error message that can be shown in front-end user pages
 *
 * @author yongnian
 */
public enum ReadableErrorDefEnum implements IErrorDef {
    /**
     * 查询正常，结果为空
     */
    QUERY_RESULT_EMPTY("QUERY_RESULT_EMPTY", "查询结果为空！"),
    QUERY_DATA_INCOMPLETE("QUERY_DATA_INCOMPLETE", "查询异常，请联系开发！"),


    /**
     * 查询数据库失败
     */
    QUERY_TBL_EXCEPTION("QUERY_TBL_EXCEPTION", "查询数据失败，请联系开发！");

    ReadableErrorDefEnum(String code, String message) {
        this.code = code;
        this.message = message;
    }

    private final String code;
    private final String message;

    @Override
    public String getCode() {
        return code;
    }


    @Override
    public String getMessage() {
        return message;
    }
}
