package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.xpipe.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author limingdong
 * @create 2021/12/17
 */
public class CompositeApplierMasterChooserAlgorithm extends AbstractApplierMasterChooserAlgorithm {

    private List<ApplierMasterChooserAlgorithm> masterChooserAlgorithms = new ArrayList<>();

    public CompositeApplierMasterChooserAlgorithm(String targetIdc, String clusterId, String backupClusterId,
                                                  CurrentMetaManager currentMetaManager, MultiDcService multiDcService, ClusterManagerConfig clusterManagerConfig, ScheduledExecutorService scheduled) {
        super(targetIdc, clusterId, backupClusterId, currentMetaManager, scheduled);
        masterChooserAlgorithms.add(new DefaultDcApplierMasterChooserAlgorithm(targetIdc, clusterId, backupClusterId, currentMetaManager, multiDcService, scheduled));
        masterChooserAlgorithms.add(new DefaultConfigApplierMasterChooserAlgorithm(targetIdc, clusterId, backupClusterId, currentMetaManager, clusterManagerConfig, scheduled));
    }

    @Override
    protected Pair<String, Integer> doChoose() {

        for (ApplierMasterChooserAlgorithm chooserAlgorithm : masterChooserAlgorithms) {
            Pair<String, Integer> replicator = chooserAlgorithm.choose();
            if( replicator!= null) {
                return replicator;
            }
        }
        return null;
    }
}
