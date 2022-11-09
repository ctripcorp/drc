package com.ctrip.framework.drc.console.vo.response;

/**
 * @ClassName QmqApiResponse
 * @Author haodongPan
 * @Date 2022/10/31 11:31
 * @Version: $
 */
public class QmqApiResponse<T> {
    
    private int status; //0:success -1:failed
    private String statusMsg;
    private T data;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getStatusMsg() {
        return statusMsg;
    }

    public void setStatusMsg(String statusMsg) {
        this.statusMsg = statusMsg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
