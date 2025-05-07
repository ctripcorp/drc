package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.cluster.impl.ClusterServerStateManager;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcService;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcServiceManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
@Component
public class DefaultMultiDcService implements MultiDcService {

    private static Logger logger = LoggerFactory.getLogger(DefaultMultiDcService.class);

    @Autowired
    private ClusterManagerMultiDcServiceManager clusterManagerMultiDcServiceManager;

    @Autowired
    private ClusterManagerConfig config;

    @Autowired
    public DataCenterService dataCenter;

    @Autowired
    private ClusterServerStateManager clusterServerStateManager;

    @Override
    public Replicator getActiveReplicator(String dcName, String clusterId) {

        Replicator replicator = doGetActiveReplicator(dcName, clusterId);
        if (replicator == null) {
            String migrationDc = config.getMigrationIdc().get(dcName);
            if (StringUtils.isNotBlank(migrationDc)) {
                replicator = doGetActiveReplicator(migrationDc, clusterId);
                logger.info("[getActiveReplicator] fail back to idc {}->{} for {} is {}", dcName, migrationDc, clusterId, replicator);
            }
        }

        return replicator;
    }

    private Replicator doGetActiveReplicator(String dcName, String clusterId) {
        String region = dataCenter.getRegion(dcName);
        RegionInfo regionInfo = config.getCmRegionInfos().get(region);
        if(regionInfo == null){
            logger.error("[getActiveReplicator][region info null]{}:{}", region, dcName);
            return null;
        }

        // check state. if not alive, should not attempt to get replicator from remote
        ServerStateEnum serverState = clusterServerStateManager.getServerState();
        if (serverState.notAlive()) {
            logger.info("[getActiveReplicator] for {}, state is not alive: {}", clusterId, serverState);
            return null;
        }

        ClusterManagerMultiDcService clusterManagerMultiDcService = clusterManagerMultiDcServiceManager.getOrCreate(regionInfo.getMetaServerAddress());
        Replicator replicator = clusterManagerMultiDcService.getActiveReplicator(clusterId);
        logger.info("[getActiveReplicator] for {} is {}", clusterId, replicator);
        return replicator;
    }
}
