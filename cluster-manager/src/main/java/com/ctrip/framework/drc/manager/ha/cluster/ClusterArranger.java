package com.ctrip.framework.drc.manager.ha.cluster;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface ClusterArranger {

    void onServerAdded(ClusterServer clusterServer);

    void onServerRemoved(ClusterServer clusterServer);

    void onServerChanged(ClusterServer oldClusterServer, ClusterServer newClusterServer);

}
