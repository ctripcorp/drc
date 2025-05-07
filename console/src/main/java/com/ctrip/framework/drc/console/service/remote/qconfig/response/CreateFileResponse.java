package com.ctrip.framework.drc.console.service.remote.qconfig.response;

/**
 * @ClassName CreateFileResponse
 * @Author haodongPan
 * @Date 2023/2/20 10:25
 * @Version: $
 */
public class CreateFileResponse {
    
    private int status;
    
    private String message;
    
    private String data;


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

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "CreateFileResponse{" +
                "status=" + status +
                ", message='" + message + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
