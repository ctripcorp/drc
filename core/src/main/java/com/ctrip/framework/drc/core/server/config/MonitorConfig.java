package com.ctrip.framework.drc.core.server.config;

import com.ctrip.framework.drc.core.driver.config.GlobalConfig;

/**
 * @Author limingdong
 * @create 2020/1/13
 */
public class MonitorConfig {

    private long clusterAppId = GlobalConfig.APP_ID;

    private String bu = GlobalConfig.BU;

    private String srcDcName = GlobalConfig.DC;

    public MonitorConfig() {
    }

    public MonitorConfig(long clusterAppId, String bu, String srcDcName) {
        this.clusterAppId = clusterAppId;
        this.bu = bu;
        this.srcDcName = srcDcName;
    }

    public long getClusterAppId() {
        return clusterAppId;
    }

    public void setClusterAppId(long clusterAppId) {
        this.clusterAppId = clusterAppId;
    }

    public String getBu() {
        return bu;
    }

    public void setBu(String bu) {
        this.bu = bu;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public void setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
    }
}
