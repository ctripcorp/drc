package com.ctrip.framework.drc.manager.ha.meta;

import com.ctrip.framework.drc.core.entity.*;

import java.util.List;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public interface DrcManager extends MetaUpdateOperation {

    Set<String> getDcs();

    Set<String> getDcClusters(String dc);

    DbCluster getCluster(String dc, String registryKey);

    List<ClusterManager> getClusterManagerServers(String dc);

    ZkServer getZkServer(String dc);

    Dc getDc(String dc);

    boolean hasCluster(String currentDc, String registryKey);

    Route randomRoute(String currentDc, String tag, Integer orgId, String dstDc);

    default Route  metaRandomRoutes(String currentDc, Integer orgId, String dstDc){
        return randomRoute(currentDc, Route.TAG_META, orgId, dstDc);
    }

}
