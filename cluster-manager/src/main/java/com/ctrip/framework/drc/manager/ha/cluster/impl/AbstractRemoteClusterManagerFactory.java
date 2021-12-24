package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;
import com.ctrip.framework.drc.manager.ha.cluster.RemoteClusterServerFactory;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public abstract class AbstractRemoteClusterManagerFactory<T extends ClusterServer> implements RemoteClusterServerFactory<T> {

    @Autowired
    private ClusterManagerConfig config;

    @Autowired
    protected T currentClusterServer;

    @Override
    public T createClusterServer(String serverId, ClusterServerInfo clusterServerInfo) {

        if(serverId != null && serverId.equalsIgnoreCase(config.getClusterServerId())){
            return currentClusterServer;
        }
        return doCreateRemoteServer(serverId, clusterServerInfo);
    }

    protected abstract T doCreateRemoteServer(String serverId, ClusterServerInfo clusterServerInfo);
}
