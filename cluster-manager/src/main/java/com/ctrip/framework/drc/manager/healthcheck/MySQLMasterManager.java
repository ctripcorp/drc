package com.ctrip.framework.drc.manager.healthcheck;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2020/5/19
 */
public interface MySQLMasterManager<V> {

    DbCluster getDbs(Endpoint endpoint);

    void updateDbs(Endpoint endpoint, DbCluster dbs);

    DbCluster getDbs(String clusterId);

    Map<Endpoint, DbCluster> getAllDbs();

    void removeDbs(String clusterId, Endpoint endpoint);

    interface CallBack<V> {
        ListenableFuture<V> onAddDbs(String clusterId, DbCluster dbs);
    }
}
