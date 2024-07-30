package com.ctrip.framework.drc.core.server.config;

import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.entity.SimpleInstance;

public abstract class InfoDto {
    private String registryKey;
    private Boolean master;
    private String ip;
    private Integer port;


    public abstract String getUpstreamIp();

    public abstract void setUpstreamIp(String upstreamIp);

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }

    public Boolean getMaster() {
        return master;
    }

    public void setMaster(Boolean master) {
        this.master = master;
    }


    public Instance mapToInstance() {
        return new SimpleInstance().setIp(this.ip).setPort(this.port).setMaster(this.master);
    }
}
