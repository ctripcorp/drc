package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.observer.Observable;

import java.util.Map;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface ClusterServers<T extends ClusterServer> extends Observable, Lifecycle {

    T currentClusterServer();

    T  getClusterServer(String serverId);

    Set<T> allClusterServers();

    void refresh() throws ClusterException;

    boolean exist(String serverId);

    Map<String, ClusterServerInfo> allClusterServerInfos();
}
