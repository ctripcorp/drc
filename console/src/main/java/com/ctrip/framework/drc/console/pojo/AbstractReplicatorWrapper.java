package com.ctrip.framework.drc.console.pojo;

import com.ctrip.framework.drc.console.monitor.delay.config.DrcReplicatorWrapper;
import com.ctrip.framework.drc.core.entity.Route;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public abstract class AbstractReplicatorWrapper implements DrcReplicatorWrapper {
    public String srcDcName;

    public String dstDcName;

    public String clusterName;

    public String mhaName;

    public String destMhaName;

    public List<Route> routes;

    public AbstractReplicatorWrapper(String srcDcName, String dstDcName, String clusterName, String mhaName, String destMhaName) {
        this(srcDcName, dstDcName, clusterName, mhaName, destMhaName, Lists.newArrayList());
    }

    public AbstractReplicatorWrapper(String srcDcName, String dstDcName, String clusterName, String mhaName, String destMhaName, List<Route> routes) {
        this.srcDcName = srcDcName;
        this.dstDcName = dstDcName;
        this.clusterName = clusterName;
        this.mhaName = mhaName;
        this.destMhaName = destMhaName;
        this.routes = routes;
    }

    @Override
    public String getDcName() {
        return srcDcName;
    }

    @Override
    public String getDestDcName() {
        return dstDcName;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public String getMhaName() {
        return mhaName;
    }

    @Override
    public String getDestMhaName() {
        return destMhaName;
    }

    @Override
    public List<Route> getRoutes() {
        return routes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractReplicatorWrapper)) {
            return false;
        }
        AbstractReplicatorWrapper that = (AbstractReplicatorWrapper) o;
        return Objects.equals(srcDcName, that.srcDcName) &&
                Objects.equals(dstDcName, that.dstDcName) &&
                Objects.equals(getClusterName(), that.getClusterName()) &&
                Objects.equals(getMhaName(), that.getMhaName()) &&
                Objects.equals(getDestMhaName(), that.getDestMhaName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcDcName, dstDcName, getClusterName(), getMhaName(), getDestMhaName());
    }
}
