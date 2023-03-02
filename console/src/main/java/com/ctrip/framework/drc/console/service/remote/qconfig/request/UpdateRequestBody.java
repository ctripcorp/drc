package com.ctrip.framework.drc.console.service.remote.qconfig.request;

import java.util.Map;

/**
 * @ClassName UpdateRequestData
 * @Author haodongPan
 * @Date 2023/2/9 15:13
 * @Version: $
 */
public class UpdateRequestBody {
    
    private String dataid;
    private Integer version;
    private Map<String,? extends Object> data;

    @Override
    public String toString() {
        return "UpdateRequestBody{" +
                "dataid='" + dataid + '\'' +
                ", version=" + version +
                ", data=" + data +
                '}';
    }

    public String getDataid() {
        return dataid;
    }

    public void setDataid(String dataid) {
        this.dataid = dataid;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Map<String,? extends Object> getData() {
        return data;
    }

    public void setData(Map<String,? extends Object> data) {
        this.data = data;
    }
}
