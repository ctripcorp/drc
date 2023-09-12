package com.ctrip.framework.drc.console.service.v2.external.dba.response;


import java.util.List;

public class DbaDbClusterInfoResponseV2 {
    private String message;
    private List<DbClusterInfoDto> data;
    private Boolean success;

    public List<DbClusterInfoDto> getData() {
        return data;
    }

    public void setData(List<DbClusterInfoDto> data) {
        this.data = data;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    @Override
    public String toString() {
        return "DbaDbClusterInfoResponseV2{" +
                "message='" + message + '\'' +
                ", data=" + data +
                ", success=" + success +
                '}';
    }
}
