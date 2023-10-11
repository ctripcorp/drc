package com.ctrip.framework.drc.console.service.v2.external.dba.response;


public class DbaApiResponse<T> {
    private String message;
    private T data;
    private Boolean success;

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
}
