package com.ctrip.framework.drc.console.enums;

/**
 * Error message that can be shown in front-end user pages
 *
 * @author yongnian
 */
public enum ReadableErrorDefEnum implements IErrorDef {

    REQUEST_PARAM_INVALID("REQUEST_PARAM_INVALID", "invalid param! "),

    /**
     * 查询正常，结果为空
     */
    QUERY_RESULT_EMPTY("QUERY_RESULT_EMPTY", "query result is empty"),
    QUERY_DATA_INCOMPLETE("QUERY_DATA_INCOMPLETE", "query exception, please contact devops"),


    /**
     * 查询数据库失败
     */
    QUERY_TBL_EXCEPTION("QUERY_TBL_EXCEPTION", "query datasource exception, please contact devops");

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
