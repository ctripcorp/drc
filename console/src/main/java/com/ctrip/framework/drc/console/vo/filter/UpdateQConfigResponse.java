package com.ctrip.framework.drc.console.vo.filter;

/**
 * Created by dengquanliang
 * 2023/4/19 14:56
 */
public class UpdateQConfigResponse {

    private int status;
    private String message;
    private String data;

    @Override
    public String toString() {
        return "UpdateConfigResponse{" +
                "status=" + status +
                ", message='" + message + '\'' +
                ", data='" + data + '\'' +
                '}';
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
