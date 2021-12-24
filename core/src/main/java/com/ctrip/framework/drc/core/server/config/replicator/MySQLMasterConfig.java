package com.ctrip.framework.drc.core.server.config.replicator;

import com.ctrip.framework.drc.core.driver.config.GlobalConfig;

/**
 * Created by mingdongli
 * 2019/9/21 下午11:06
 */
public class MySQLMasterConfig implements GlobalConfig {

    private String ip;

    private int port;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
