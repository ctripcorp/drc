package com.ctrip.framework.drc.manager.ha.meta;

import com.ctrip.framework.drc.core.entity.DbCluster;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public interface MetaUpdateOperation {

    DbCluster removeCluster(String currentDc, String registryKey);

    void update(String dcId, DbCluster clusterMeta);

}
