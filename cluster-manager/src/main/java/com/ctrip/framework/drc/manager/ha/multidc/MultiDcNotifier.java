package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.StateChangeHandler;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcService;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcServiceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.NOTIFY_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.STATE_LOGGER;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
@Order(1)
public class MultiDcNotifier implements StateChangeHandler {

    @Autowired
    private ClusterManagerConfig config;

    @Resource( name = AbstractSpringConfigContext.GLOBAL_EXECUTOR )
    private ExecutorService executors;

    @Autowired
    private ClusterManagerMultiDcServiceManager clusterManagerMultiDcServiceManager;

    @Autowired
    public RegionCache regionMetaCache;

    @Autowired
    public DataCenterService dataCenter;

    @Override
    public void replicatorActiveElected(String clusterId, Replicator activeReplicator) {
        if (activeReplicator == null) {
            STATE_LOGGER.info("[{}][replicatorActiveElected][none replicator survive, do nothing]", getClass().getSimpleName());
            return;
        }

        Map<String, RegionInfo> regionInfos = config.getCmRegionInfos();
        Map<String, String> backupDcs = regionMetaCache.getBackupDcs(clusterId); //dcName, clusterName.mhaName
        NOTIFY_LOGGER.info("[replicatorActiveElected][notify backup dc]{}, {}, {}", clusterId, backupDcs, activeReplicator);
        for (Map.Entry<String, String> entry : backupDcs.entrySet()) {

            String backupDcName = entry.getKey();
            String backupClusterId = entry.getValue();
            String region = dataCenter.getRegion(entry.getKey());
            RegionInfo regionInfo = regionInfos.get(region);

            if (regionInfo == null) {
                NOTIFY_LOGGER.error("[replicatorActiveElected][backup dc, but can not find region info]{}, {}, {}", backupDcName, backupClusterId, regionInfos);
                continue;
            }
            ClusterManagerMultiDcService clusterManagerMultiDcService = clusterManagerMultiDcServiceManager.getOrCreate(regionInfo.getMetaServerAddress());
            executors.execute(new BackupDcNotifyTask(clusterManagerMultiDcService, clusterId, backupClusterId, activeReplicator));
        }

    }

    @Override
    public void messengerActiveElected(String clusterId, Messenger messenger) {
    }

    @Override
    public void applierActiveElected(String clusterId, Applier applier) {

    }

    @Override
    public void applierMasterChanged(String clusterId, String backupClusterId, Pair<String, Integer> newMaster) {

    }

    @Override
    public void mysqlMasterChanged(String clusterId, Endpoint endpoint) {

    }

    public class BackupDcNotifyTask extends AbstractExceptionLogTask {

        private ClusterManagerMultiDcService clusterManagerMultiDcService;

        private String clusterId;

        private String backupClusterId;

        private Replicator activeReplicator;

        public BackupDcNotifyTask(ClusterManagerMultiDcService clusterManagerMultiDcService, String clusterId, String backupClusterId, Replicator activeReplicator) {
            this.clusterManagerMultiDcService = clusterManagerMultiDcService;
            this.clusterId = clusterId;
            this.backupClusterId = backupClusterId;
            this.activeReplicator = activeReplicator;
        }

        @Override
        protected void doRun() {

            NOTIFY_LOGGER.info("[doRun]{}, {}, {}, {}", clusterManagerMultiDcService, clusterId, backupClusterId, activeReplicator);
            clusterManagerMultiDcService.upstreamChange(clusterId, backupClusterId, activeReplicator.getIp(), activeReplicator.getApplierPort());  // clusterName.
        }

    }

    public void setExecutors(ExecutorService executors) {
        this.executors = executors;
    }
}
