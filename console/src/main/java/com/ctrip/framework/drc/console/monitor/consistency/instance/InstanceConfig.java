package com.ctrip.framework.drc.console.monitor.consistency.instance;

import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.core.monitor.entity.ConsistencyEntity;
import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by mingdongli
 * 2019/11/19 下午2:59.
 */
public class InstanceConfig {

    private Endpoint srcEndpoint;

    private Endpoint dstEndpoint;

    private String cluster;

    private DelayMonitorConfig delayMonitorConfig;

    private ConsistencyEntity consistencyEntity;

    public Endpoint getSrcEndpoint() {
        return srcEndpoint;
    }

    public void setSrcEndpoint(Endpoint srcEndpoint) {
        this.srcEndpoint = srcEndpoint;
    }

    public Endpoint getDstEndpoint() {
        return dstEndpoint;
    }

    public void setDstEndpoint(Endpoint dstEndpoint) {
        this.dstEndpoint = dstEndpoint;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public DelayMonitorConfig getDelayMonitorConfig() {
        return delayMonitorConfig;
    }

    public void setDelayMonitorConfig(DelayMonitorConfig delayMonitorConfig) {
        this.delayMonitorConfig = delayMonitorConfig;
    }

    public ConsistencyEntity getConsistencyEntity() {
        return consistencyEntity;
    }

    public void setConsistencyEntity(ConsistencyEntity consistencyEntity) {
        this.consistencyEntity = consistencyEntity;
    }
}
