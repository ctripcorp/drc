package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.config.GlobalConfig;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.RouteUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        DelayMonitorSlaveConfig config = (DelayMonitorSlaveConfig) o;
        return Objects.equals(dc, config.dc) &&
                Objects.equals(destDc, config.destDc) &&
                Objects.equals(cluster, config.cluster) &&
                Objects.equals(mha, config.mha) &&
                Objects.equals(destMha, config.destMha) &&
                Objects.equals(measurement, config.measurement) &&
                Objects.equals(routeInfo, config.routeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dc, destDc, cluster, mha, destMha, measurement, routeInfo);
    }

    public DelayMonitorSlaveConfig clone() {
        DelayMonitorSlaveConfig config = new DelayMonitorSlaveConfig();
        config.setDc(this.dc);
        config.setDestDc(this.destDc);
        config.setCluster(this.cluster);
        config.setMha(this.mha);
        config.setDestMha(this.destMha);
        config.setRegistryKey(getRegistryKey());
        Endpoint endpoint = new DefaultEndPoint(getIp(), getPort());
        config.setEndpoint(endpoint);
        config.setMeasurement(this.measurement);
        config.setRouteInfo(this.routeInfo);
        return config;
    }
}
