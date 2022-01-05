package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.DcManager;
import com.ctrip.framework.drc.manager.ha.meta.impl.DefaultDcManager;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.framework.drc.manager.ha.meta.impl.DefaultDcCache.MEMORY_META_SERVER_DAO_KEY;

/**
 * @Author limingdong
 * @create 2021/12/17
 */
public class DefaultConfigApplierMasterChooserAlgorithm extends AbstractApplierMasterChooserAlgorithm {

    private ClusterManagerConfig clusterManagerConfig;

    public DefaultConfigApplierMasterChooserAlgorithm(String targetIdc, String clusterId, String backupClusterId,
                                                      CurrentMetaManager currentMetaManager, ClusterManagerConfig clusterManagerConfig, ScheduledExecutorService scheduled) {
        super(targetIdc, clusterId, backupClusterId, currentMetaManager, scheduled);
        this.clusterManagerConfig = clusterManagerConfig;
    }

    @Override
    protected Pair<String, Integer> doChoose() {
        Pair<String, Integer> replicator = clusterManagerConfig.getApplierMaster(backupClusterId + "." + targetIdc);
        if (replicator != null) {
            return replicator;
        }
        String fileName = System.getProperty(MEMORY_META_SERVER_DAO_KEY, "memory_meta_server_dao_file.xml");
        if (fileName != null) {
            logger.info("[loadMetaManager][load from file]{}", fileName);
            DcManager dcMetaManager = DefaultDcManager.buildFromFile(targetIdc, fileName);
            DbCluster dbCluster = dcMetaManager.getCluster(backupClusterId);
            if (dbCluster != null) {
                List<Replicator> replicators = dbCluster.getReplicators();
                for (Replicator r : replicators) {
                    if (r.isMaster()) {
                        logger.debug("[doChooseApplierMaster]{}, {}, {}, {}", targetIdc, clusterId, backupClusterId, r);
                        return new Pair<>(r.getIp(), r.getApplierPort());
                    }
                }
            }
        }
        return null;
    }
}
