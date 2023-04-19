package com.ctrip.framework.drc.console.service.remote.qconfig.response;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/4/19 16:18
 */
public class QConfigVersion {
    private String dataId;
    private int version;

    @Override
    public String toString() {
        return "QConfigVersion{" +
                "dataId='" + dataId + '\'' +
                ", version=" + version +
                '}';
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
