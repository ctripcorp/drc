package com.ctrip.framework.drc.console.vo.filter;

/**
 * Created by dengquanliang
 * 2023/4/24 14:36
 */
public class QConfigDataResponse {

    private int status;
    private String message;
    private QConfigDetailData data;

    public boolean exist() {
        return this.status == 0;
    }

    @Override
    public String toString() {
        return "QConfigDataResponse{" +
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

    public QConfigDetailData getData() {
        return data;
    }

    public void setData(QConfigDetailData data) {
        this.data = data;
    }
}
