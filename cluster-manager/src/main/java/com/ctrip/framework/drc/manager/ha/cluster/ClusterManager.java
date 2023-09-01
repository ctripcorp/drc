package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.rest.ForwardInfo;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface ClusterManager extends ClusterServer, TopElement {

    void clusterAdded(String dcId, String clusterId, ForwardInfo forwardInfo, String operator);

    void clusterModified(String clusterId, ForwardInfo forwardInfo, String operator);

    void clusterDeleted(String clusterId, ForwardInfo forwardInfo, String operator);

    void updateUpstream(String clusterId, String backupClusterId, String ip, int port, ForwardInfo forwardInfo);

    Replicator getActiveReplicator(String clusterId, ForwardInfo forwardInfo);

    Endpoint getActiveMySQL(String clusterId, ForwardInfo forwardInfo);

    String getCurrentMeta();

}
