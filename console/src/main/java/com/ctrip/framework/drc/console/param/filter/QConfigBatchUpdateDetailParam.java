package com.ctrip.framework.drc.console.param.filter;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/4/25 10:50
 */
public class QConfigBatchUpdateDetailParam {

    private int version;
    private Map<String, String> configMap;

    @Override
    public String toString() {
        return "QConfigBatchUpdateDetail{" +
                "version=" + version +
                ", configMap=" + configMap +
                '}';
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Map<String, String> getConfigMap() {
        return configMap;
    }

    public void setConfigMap(Map<String, String> configMap) {
        this.configMap = configMap;
    }
}
