package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterManager;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;
import org.springframework.stereotype.Component;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
@Component
public class DefaultRemoteClusterManagerFactory extends AbstractRemoteClusterManagerFactory<ClusterManager> {

    @Override
    protected ClusterManager doCreateRemoteServer(String serverId, ClusterServerInfo clusterServerInfo) {

        return new RemoteClusterManager(currentClusterServer.getServerId(), serverId, clusterServerInfo);
    }

}
