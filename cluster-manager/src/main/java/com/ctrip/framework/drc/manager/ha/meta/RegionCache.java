package com.ctrip.framework.drc.manager.ha.meta;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.manager.ha.meta.impl.DefaultDcCache;
import com.ctrip.xpipe.api.observer.Observable;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jixinwang on 2022/8/2
 */
public interface RegionCache extends Observable {

    Set<String> getClusters();

    Map<String, String> getBackupDcs(String clusterId);

    DbCluster getCluster(String registryKey);

    Route randomRoute(String clusterId, String dstDc);

    void clusterAdded(String dcId, DbCluster dbCluster);

    void clusterModified(DbCluster dbCluster);

    void clusterDeleted(String registryKey);

    List<DefaultDcCache> getDcCaches();

}
