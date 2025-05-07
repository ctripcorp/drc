package com.ctrip.framework.drc.manager.ha.meta;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.xpipe.api.observer.Observable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public interface DcCache extends Observable {

    Set<String> getClusters();

    Map<String, String> getBackupDcs(String clusterId);

    DbCluster getCluster(String registryKey);

    Route randomRoute(String clusterId, String dstDc);

    Route randomRoute(String clusterId,String dstDc,Integer orgId);

    void clusterAdded(String clusterId);

    void clusterModified(String clusterId);

    void clusterDeleted(String registryKey);

    void refresh(String clusterId);

    Future<Boolean> triggerRefreshAll();
}
