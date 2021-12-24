package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.entity.Replicator;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public interface MultiDcService {

    Replicator getActiveReplicator(String dcName, String clusterId);

}
