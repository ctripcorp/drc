package com.ctrip.framework.drc.console.service.v2.external.dba.response;


/**
 * @ClassName ClusterApiResponse
 * @Author haodongPan
 * @Date 2023/8/25 10:19
 * @Version: $
 */
public class DbaClusterInfoResponse  {
    private String message;
    private Data data;
    private boolean success;
    public void setMessage(String message) {
        this.message = message;
    }
    public String getMessage() {
        return message;
    }

    public void setData(Data data) {
        this.data = data;
    }
    public Data getData() {
        return data;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
    public boolean getSuccess() {
        return success;
    }
}
