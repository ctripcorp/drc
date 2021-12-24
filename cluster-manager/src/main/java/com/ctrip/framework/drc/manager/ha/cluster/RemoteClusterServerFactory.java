package com.ctrip.framework.drc.manager.ha.cluster;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface RemoteClusterServerFactory<T extends ClusterServer> {

    T  createClusterServer(String serverId, ClusterServerInfo clusterServerInfo);

}
