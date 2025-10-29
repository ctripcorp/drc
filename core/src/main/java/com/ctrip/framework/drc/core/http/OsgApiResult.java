package com.ctrip.framework.drc.core.http;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;

/**
 * Created by shiruixin
 * 2025/8/7 09:47
 */
public class OsgApiResult<T> {
    private boolean success;
    private String message;

    private T data;

    public static <T> OsgApiResult getInstance( T data, boolean success, String message) {
        OsgApiResult<T> result = new OsgApiResult<T>();
        result.setData(data);
        result.setSuccess(success);
        result.setMessage(message);
        return result;
    }

    public static <T> OsgApiResult getSuccessInstance( T data) {
        return getInstance(data, true, ResultCode.OSG_HANDLE_SUCCESS.getMessage());
    }

    public static <T> OsgApiResult getSuccessInstance( T data,String message) {
        return getInstance(data, true, message);
    }

    public static <T> OsgApiResult getFailInstance( T data) {
        return getInstance(data, false, ResultCode.HANDLE_FAIL.getMessage());
    }

    public static <T> OsgApiResult getFailInstance( T data,String message) {
        return getInstance(data, false, message);
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

}
