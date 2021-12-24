package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.DcInfo;
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
        dcName = dcName.toLowerCase();
        DcInfo dcInfo = config.getDcInofs().get(dcName);
        if(dcInfo == null){
            logger.error("[getActiveReplicator][dc info null]{}", dcName);
            return null;
        }

        ClusterManagerMultiDcService clusterManagerMultiDcService = clusterManagerMultiDcServiceManager.getOrCreate(dcInfo.getMetaServerAddress());
        Replicator replicator = clusterManagerMultiDcService.getActiveReplicator(clusterId);
        logger.info("[getActiveReplicator] for {} is {}", clusterId, replicator);
        return replicator;
    }
}
