package com.ctrip.framework.drc.manager.ha;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;

/**
 * event should handle
 * @Author limingdong
 * @create 2020/4/29
 */
public interface StateChangeHandler {

    void replicatorActiveElected(String clusterId, Replicator replicator);  // notify all replicators in local dc and applier in other dc

    void messengerActiveElected(String clusterId, Messenger messenger);  // notify all messenger in local dc

    void applierMasterChanged(String clusterId, String backupClusterId, Pair<String, Integer> newMaster);  // notify active applier in local dc

    void applierActiveElected(String clusterId, Applier applier); // notify applier in local dc

    void mysqlMasterChanged(String clusterId, Endpoint master); // notify replicator and appliers in local dc
}
