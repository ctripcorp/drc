package com.ctrip.framework.drc.core.server.config.replicator.dto;

import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.config.InfoDto;

public class ReplicatorInfoDto extends InfoDto {
    private String upstreamMasterIp;

    private int applierPort;

    public int getApplierPort() {
        return applierPort;
    }

    public void setApplierPort(int applierPort) {
        this.applierPort = applierPort;
    }

    public String getUpstreamMasterIp() {
        return upstreamMasterIp;
    }

    public void setUpstreamMasterIp(String upstreamMasterIp) {
        this.upstreamMasterIp = upstreamMasterIp;
    }

    public Instance mapToReplicatorInstance() {
        return new Replicator().setIp(this.getIp()).setPort(this.getPort()).setMaster(this.getMaster());
    }

    @Override
    public String getUpstreamIp() {
        return getUpstreamMasterIp();
    }

    @Override
    public void setUpstreamIp(String upstreamIp) {
        setUpstreamMasterIp(upstreamIp);
    }
}
