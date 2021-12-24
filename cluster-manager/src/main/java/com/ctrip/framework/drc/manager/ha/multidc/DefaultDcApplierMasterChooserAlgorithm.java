package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public class DefaultDcApplierMasterChooserAlgorithm extends AbstractApplierMasterChooserAlgorithm {

    private MultiDcService multiDcService;

    public DefaultDcApplierMasterChooserAlgorithm(String targetIdc, String clusterId, String backupClusterId,
                                                CurrentMetaManager currentMetaManager, MultiDcService multiDcService, ScheduledExecutorService scheduled) {
        super(targetIdc, clusterId, backupClusterId, currentMetaManager, scheduled);
        this.multiDcService = multiDcService;
    }

    @Override
    protected Pair<String, Integer> doChoose() {

        Replicator replicator = multiDcService.getActiveReplicator(targetIdc, backupClusterId);
        logger.debug("[doChooseKeeperMaster]{}, {}, {}, {}", targetIdc, clusterId, backupClusterId, replicator);
        if(replicator == null){
            return null;
        }
        return new Pair<>(replicator.getIp(), replicator.getApplierPort());
    }
}
