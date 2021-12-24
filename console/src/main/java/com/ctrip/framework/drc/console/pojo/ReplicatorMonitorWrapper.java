package com.ctrip.framework.drc.console.pojo;

import com.ctrip.framework.drc.console.monitor.delay.config.DrcReplicatorWrapper;
import com.ctrip.framework.drc.core.entity.ReplicatorMonitor;

public class ReplicatorMonitorWrapper extends AbstractReplicatorWrapper implements DrcReplicatorWrapper {
    public ReplicatorMonitor replicatorMonitor;

    public ReplicatorMonitorWrapper(ReplicatorMonitor replicatorMonitor, String srcDcName, String dstDcName, String clusterName, String mhaName, String destMhaName) {
        super(srcDcName, dstDcName, clusterName, mhaName, destMhaName);
        this.replicatorMonitor = replicatorMonitor;
    }

    @Override
    public String getIp() {
        return replicatorMonitor.getIp();
    }

    @Override
    public int getPort() {
        return replicatorMonitor.getApplierPort();
    }
}