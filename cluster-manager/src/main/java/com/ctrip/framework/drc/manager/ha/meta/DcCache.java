package com.ctrip.framework.drc.manager.ha.meta;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.xpipe.api.observer.Observable;

import java.util.Map;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public interface DcCache extends Observable {

    Set<String> getClusters();

    Map<String, String> getBackupDcs(String clusterId);

    DbCluster getCluster(String registryKey);

    Route randomRoute(String clusterId, String dstDc);

    DcManager getDcMeta();

    void clusterAdded(DbCluster dbCluster);

    void clusterModified(DbCluster dbCluster);

    void clusterDeleted(String registryKey);

}
