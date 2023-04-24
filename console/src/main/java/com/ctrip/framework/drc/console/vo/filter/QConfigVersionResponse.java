package com.ctrip.framework.drc.console.vo.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/19 16:17
 */
public class QConfigVersionResponse {
    private int status;
    private String message;

    private List<QConfigVersion> data;

    @Override
    public String toString() {
        return "QConfigVersionResponse{" +
                "status=" + status +
                ", message='" + message + '\'' +
                ", data=" + data +
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

    public List<QConfigVersion> getData() {
        return data;
    }

    public void setData(List<QConfigVersion> data) {
        this.data = data;
    }
}
