package com.ctrip.framework.drc.console.pojo;

import com.ctrip.framework.drc.console.monitor.delay.config.DrcReplicatorWrapper;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.entity.Route;

import java.util.List;
import java.util.Objects;

public class ReplicatorWrapper extends AbstractReplicatorWrapper implements DrcReplicatorWrapper {
    public Replicator replicator;

    public ReplicatorWrapper(Replicator replicator, String srcDcName, String dstDcName, String clusterName, String mhaName, String destMhaName, List<Route> routes) {
        super(srcDcName, dstDcName, clusterName, mhaName, destMhaName, routes);
        this.replicator = replicator;
    }

    @Override
    public String getIp() {
        return replicator.getIp();
    }

    @Override
    public int getPort() {
        return replicator.getApplierPort();
    }

    public Replicator getReplicator() {
        return replicator;
    }

    public void setReplicator(Replicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReplicatorWrapper)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ReplicatorWrapper that = (ReplicatorWrapper) o;
        return Objects.equals(getReplicator(), that.getReplicator());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getReplicator());
    }
}
