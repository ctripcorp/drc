package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public abstract class AbstractApplierMasterChooser extends AbstractStartStoppable implements ApplierMasterChooser {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static int DEFAULT_KEEPER_MASTER_CHECK_INTERVAL_SECONDS = Integer
            .parseInt(System.getProperty("KEEPER_MASTER_CHECK_INTERVAL_SECONDS", "5"));

    protected CurrentMetaManager currentMetaManager;

    protected ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    protected String targetIdc;

    protected String clusterId, backupClusterId;

    protected int checkIntervalSeconds;

    public AbstractApplierMasterChooser(String targetIdc, String clusterId, String backupClusterId,
                                       CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
        this(targetIdc, clusterId, backupClusterId, currentMetaManager, scheduled,
                DEFAULT_KEEPER_MASTER_CHECK_INTERVAL_SECONDS);
    }

    public AbstractApplierMasterChooser(String targetIdc, String clusterId, String backupClusterId, CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled, int checkIntervalSeconds) {

        this.targetIdc = targetIdc;
        this.currentMetaManager = currentMetaManager;
        this.scheduled = scheduled;
        this.clusterId = clusterId;
        this.backupClusterId = backupClusterId;
        this.checkIntervalSeconds = checkIntervalSeconds;
    }

    @Override
    protected void doStart() throws Exception {

        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {

                Pair<String, Integer> keeperMaster = chooseApplierMaster();
                logger.debug("[doRun]{}, {}, {}", clusterId, backupClusterId, keeperMaster);
                Pair<String, Integer> currentMaster = currentMetaManager.getApplierMaster(clusterId, backupClusterId);
                if (keeperMaster == null || keeperMaster.equals(currentMaster)) {
                    logger.debug("[doRun][new master null or equals old master]{}", keeperMaster);
                    return;
                }
                logger.debug("[doRun][set]{}, {}, {}", clusterId, backupClusterId, keeperMaster);
                currentMetaManager.setApplierMaster(clusterId, backupClusterId, keeperMaster.getKey(), keeperMaster.getValue());
            }


        }, 0, checkIntervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() throws Exception {

        if (future != null) {
            logger.info("[doStop]");
            future.cancel(true);
        }

    }

    protected abstract Pair<String, Integer> chooseApplierMaster();

    @Override
    public void release() throws Exception {
        stop();
    }

    //for test
    protected ScheduledFuture<?> getFuture() {
        return future;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s", getClass().getSimpleName(), clusterId, backupClusterId);
    }
}
