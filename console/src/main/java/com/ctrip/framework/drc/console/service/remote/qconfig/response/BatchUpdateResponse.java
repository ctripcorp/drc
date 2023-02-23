package com.ctrip.framework.drc.console.service.remote.qconfig.response;

/**
 * @ClassName BatchUpdateResponse
 * @Author haodongPan
 * @Date 2023/2/9 11:37
 * @Version: $
 * API 5.4.7
 */
public class BatchUpdateResponse {
    
    private int status;
    
    private String message;
    
    private String data;
    

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
