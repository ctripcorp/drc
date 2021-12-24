package com.ctrip.framework.drc.performance.activity;

import com.ctrip.framework.drc.applier.activity.event.CommitActivity;
import com.ctrip.framework.drc.applier.event.transaction.Transaction;
import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.enums.DirectionEnum;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.performance.impl.CommitMonitorReport;

/**
 * Created by jixinwang on 2021/8/17
 */
public class LocalCommitActivity extends CommitActivity {

    private TrafficEntity trafficEntity = new TrafficEntity.Builder()
            .clusterAppId(100023500L)
            .buName("BBZ")
            .dcName("SHAOY")
            .clusterName("performance_tools")
            .ip("10.2.66.144")
            .port(1234)
            .direction(DirectionEnum.OUT.getDescription())
            .module(ModuleEnum.REPLICATOR.getDescription())
            .build();

    private CommitMonitorReport commitMonitorReport = new CommitMonitorReport(100023500L, trafficEntity);

    public LocalCommitActivity() {
        try {
            commitMonitorReport.initialize();
            commitMonitorReport.start();
        } catch (Exception e) {
            logger.error("initialize commitMonitorReport error");
        }
    }

    @Override
    public Transaction doTask(Transaction transaction) throws InterruptedException {
        commitMonitorReport.addOneCount();
        return super.doTask(transaction);
    }

    @Override
    public void doStop() {
        try {
            commitMonitorReport.stop();
        } catch (Exception e) {
            logger.error("stop commitMonitorReport error");
        }
        super.doStop();
    }

    @Override
    protected void doDispose() throws Exception {
        commitMonitorReport.dispose();
       super.doDispose();
    }
}
