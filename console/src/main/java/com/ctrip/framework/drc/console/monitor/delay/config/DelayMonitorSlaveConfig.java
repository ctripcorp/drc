package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.core.driver.config.GlobalConfig;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-07
 */
public class DelayMonitorSlaveConfig extends MySQLSlaveConfig implements GlobalConfig  {

    private String dc;

    private String destDc;

    private String cluster;

    private String mha;

    private String destMha;

    private String measurement;

    private String routeInfo;

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public String getDestDc() {
        return destDc;
    }

    public void setDestDc(String destDc) {
        this.destDc = destDc;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getMha() {
        return mha;
    }

    public void setMha(String mha) {
        this.mha = mha;
    }

    public String getDestMha() {
        return destMha;
    }

    public void setDestMha(String destMha) {
        this.destMha = destMha;
    }

    public String getMeasurement() {
        return measurement;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public String getIp() {
        return this.getEndpoint().getHost();
    }

    public Integer getPort() {
        return this.getEndpoint().getPort();
    }

    public String getRouteInfo() {
        return routeInfo;
    }

    public void setRouteInfo(String routeInfo) {
        this.routeInfo = routeInfo;
    }
}
