package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public abstract class AbstractApplierMasterChooserAlgorithm implements ApplierMasterChooserAlgorithm {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected CurrentMetaManager currentMetaManager;

    protected String targetIdc;

    protected String clusterId, backupClusterId;

    protected ScheduledExecutorService scheduled;


    public AbstractApplierMasterChooserAlgorithm(String targetIdc, String clusterId, String backupClusterId, CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
        this.targetIdc = targetIdc;
        this.currentMetaManager = currentMetaManager;
        this.clusterId = clusterId;
        this.backupClusterId = backupClusterId;
        this.scheduled = scheduled;
    }

    @Override
    public Pair<String, Integer> choose() {
        return doChoose();
    }

    protected abstract Pair<String, Integer> doChoose();

}
