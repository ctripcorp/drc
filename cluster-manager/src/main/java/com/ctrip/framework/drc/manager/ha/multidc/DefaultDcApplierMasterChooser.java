package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public class DefaultDcApplierMasterChooser extends AbstractApplierMasterChooser {

    private MultiDcService multiDcService;

    private ClusterManagerConfig clusterManagerConfig;

    private ApplierMasterChooserAlgorithm applierMasterChooserAlgorithm;

    public DefaultDcApplierMasterChooser(String targetIdc, String clusterId, String backupClusterId, MultiDcService multiDcService,
                                         CurrentMetaManager currentMetaManager, ClusterManagerConfig clusterManagerConfig,  ScheduledExecutorService scheduled) {
        this(targetIdc, clusterId, backupClusterId, multiDcService, currentMetaManager, clusterManagerConfig, scheduled, DEFAULT_KEEPER_MASTER_CHECK_INTERVAL_SECONDS);
    }

    public DefaultDcApplierMasterChooser(String targetIdc, String clusterId, String backupClusterId, MultiDcService multiDcService,
                                        CurrentMetaManager currentMetaManager, ClusterManagerConfig clusterManagerConfig, ScheduledExecutorService scheduled, int checkIntervalSeconds) {
        super(targetIdc, clusterId, backupClusterId, currentMetaManager, scheduled, checkIntervalSeconds);
        this.multiDcService = multiDcService;
        this.clusterManagerConfig = clusterManagerConfig;
    }

    @Override
    protected Pair<String, Integer> chooseApplierMaster() {

        if (applierMasterChooserAlgorithm == null) {
            applierMasterChooserAlgorithm = new CompositeApplierMasterChooserAlgorithm(targetIdc, clusterId, backupClusterId, currentMetaManager, multiDcService, clusterManagerConfig, scheduled);
        }

        return applierMasterChooserAlgorithm.choose();
    }

}
