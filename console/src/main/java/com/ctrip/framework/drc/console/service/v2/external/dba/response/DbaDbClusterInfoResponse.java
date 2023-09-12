package com.ctrip.framework.drc.console.service.v2.external.dba.response;


import java.util.List;

/**
 * @ClassName ClusterApiResponse
 * @Author haodongPan
 * @Date 2023/8/25 10:19
 * @Version: $
 */
public class DbaDbClusterInfoResponse {
    private String message;
    private List<ClusterInfoDto> data;
    private boolean success;

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public List<ClusterInfoDto> getData() {
        return data;
    }

    public void setData(List<ClusterInfoDto> data) {
        this.data = data;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public boolean getSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "DbaDbClusterInfoResponse{" +
                "message='" + message + '\'' +
                ", data=" + data +
                ", success=" + success +
                '}';
    }
}
