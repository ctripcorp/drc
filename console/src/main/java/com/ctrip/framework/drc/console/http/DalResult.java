package com.ctrip.framework.drc.console.http;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-11-13
 */
public class DalResult<T> {

    private Integer status;

    private String message;

    private T result;

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }
}
