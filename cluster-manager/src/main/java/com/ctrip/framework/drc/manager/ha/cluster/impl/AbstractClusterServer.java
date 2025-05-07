package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public abstract class AbstractClusterServer extends AbstractLifecycleObservable implements ClusterServer {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private String serverId;

    private ClusterServerInfo clusterServerInfo;

    public AbstractClusterServer() {

    }

    public AbstractClusterServer(String serverId, ClusterServerInfo clusterServerInfo) {

        this.serverId = serverId;
        this.clusterServerInfo = clusterServerInfo;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(serverId);
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public void setClusterServerInfo(ClusterServerInfo clusterServerInfo) {
        this.clusterServerInfo = clusterServerInfo;
    }

    @Override
    public boolean equals(Object obj) {

        if(!(obj instanceof ClusterServer)){
            return false;
        }

        return ObjectUtils.equals(this, (ClusterServer)obj, new ObjectUtils.EqualFunction<ClusterServer>(){

            @Override
            public boolean equals(ClusterServer obj1, ClusterServer obj2) {
                return obj1.getServerId().equalsIgnoreCase(obj2.getServerId());
            }

        });
    }

    @Override
    public String getServerId() {
        return this.serverId;
    }

    @Override
    public ClusterServerInfo getClusterInfo() {
        return this.clusterServerInfo;
    }

    protected synchronized void updateClusterState(ServerStateEnum stateEnum) {
        this.clusterServerInfo.setState(stateEnum);
    }

    @Override
    public String toString() {
        return String.format("serverId:%s, (%s)", serverId, clusterServerInfo);
    }

}
