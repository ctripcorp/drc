package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;

/**
 * return value for unit test
 *
 * @Author limingdong
 * @create 2020/5/5
 */
public interface InstanceStateController extends Lifecycle {

    DbCluster addReplicator(String clusterId, Replicator replicator);

    DbCluster registerReplicator(String clusterId, Replicator replicator);

    void removeReplicator(String clusterId, Replicator replicator);

    DbCluster addMessenger(String clusterId, Messenger messenger);

    DbCluster registerMessenger(String clusterId, Messenger messenger);

    void removeMessenger(String clusterId, Messenger messenger, boolean delete);

    DbCluster addApplier(String clusterId, Applier applier);

    DbCluster registerApplier(String clusterId, Applier applier);

    Pair<List<String>, List<ApplierInfoDto>> getMessengerInfo(List<? extends Instance> messengers);

    Pair<List<String>, List<ApplierInfoDto>> getApplierInfo(List<? extends Instance> appliers);

    Pair<List<String>, List<ReplicatorInfoDto>> getReplicatorInfo(List<? extends Instance> appliers);

    void removeApplier(String clusterId, Applier applier, boolean delete);

    DbCluster applierMasterChange(String clusterId, Pair<String, Integer> newMaster, Applier applier);

    DbCluster applierPropertyChange(String clusterId, Applier applier);

    DbCluster messengerPropertyChange(String clusterId, Messenger messenger);

    List<DbCluster> mysqlMasterChanged(String clusterId, Endpoint endpoint, List<Applier> activeApplier, Replicator replicator);
}
