package com.ctrip.framework.drc.manager.ha.meta;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Route;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public interface DcManager {

    /**
     * if no route found return null
     * @param clusterId
     * @return
     */
    Route randomRoute(String clusterId, String dstDc);

    Route randomRoute(String clusterId, String dstDc, Integer orgId);

    Set<String> getClusters();

    boolean hasCluster(String registryKey);

    DbCluster getCluster(String registryKey);

    Dc getDc();

    void update(DbCluster dbCluster);

    DbCluster removeCluster(String registryKey);
    
}
