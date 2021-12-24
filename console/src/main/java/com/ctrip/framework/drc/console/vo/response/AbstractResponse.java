package com.ctrip.framework.drc.console.vo.response;

/**
 * Created by jixinwang on 2021/7/13
 */
public class AbstractResponse<T> {
    private Integer status;

    private String message;

    private T data;

    public Integer getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public T getData() {
        return data;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setData(T data) {
        this.data = data;
    }
}
