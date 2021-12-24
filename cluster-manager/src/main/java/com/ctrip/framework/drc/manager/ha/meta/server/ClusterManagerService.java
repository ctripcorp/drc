package com.ctrip.framework.drc.manager.ha.meta.server;

import com.ctrip.framework.drc.core.entity.Replicator;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public interface ClusterManagerService {

    String HTTP_HEADER_FOWRARD = "forward";

    Replicator getActiveReplicator(String clusterId);

}
