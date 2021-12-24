package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;

/**
 * return value for unit test
 * @Author limingdong
 * @create 2020/5/5
 */
public interface InstanceStateController extends Lifecycle {

    DbCluster addReplicator(String clusterId, Replicator replicator);

    DbCluster registerReplicator(String clusterId, Replicator replicator);

    void removeReplicator(String clusterId, Replicator replicator);

    DbCluster addApplier(String clusterId, Applier applier);

    DbCluster registerApplier(String clusterId, Applier applier);

    void removeApplier(String clusterId, Applier applier, boolean delete);

    DbCluster applierMasterChange(String clusterId, Pair<String, Integer> newMaster, Applier applier);

    List<DbCluster> mysqlMasterChanged(String clusterId, Endpoint endpoint, List<Applier> activeApplier, Replicator replicator);
}
