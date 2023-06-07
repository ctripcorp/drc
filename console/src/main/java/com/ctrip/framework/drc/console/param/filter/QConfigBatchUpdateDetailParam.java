package com.ctrip.framework.drc.console.param.filter;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/4/25 10:50
 */
public class QConfigBatchUpdateDetailParam {

    private String dataid;
    private int version;
    private Map<String, String> data;

    @Override
    public String toString() {
        return "QConfigBatchUpdateDetailParam{" +
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

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }
}
