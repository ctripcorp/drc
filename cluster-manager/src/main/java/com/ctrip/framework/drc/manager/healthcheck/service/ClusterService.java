package com.ctrip.framework.drc.manager.healthcheck.service;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Created by mingdongli
 * 2019/11/21 下午5:13.
 */
public interface ClusterService<V> extends Lifecycle {

    DbCluster getDbCluster(Endpoint endpoint);

    DbCluster getDbCluster(String registryPath);

    /**
     * added by console through api
     * set uuid of db in zoneInfo, add master to heartbeat map
     * @param dbCluster
     */
    ListenableFuture<V> addDbCluster(DbCluster dbCluster);

    /**
     * updated by scheduled task
     * @param master
     * @param dbCluster
     */
    void updateDbCluster(Endpoint master, DbCluster dbCluster);

    DbCluster removeDbCluster(Endpoint endpoint);

    void updateDrc(Drc drc);

    Drc getDrc();

    interface CallBack<V> {
        ListenableFuture<V> onAddDbCluster(DbCluster dbCluster);
    }

}
