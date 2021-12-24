package com.ctrip.framework.drc.performance.impl;

import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.kpi.AbstractMonitorReport;

/**
 * Created by jixinwang on 2021/8/17
 */
public class CommitMonitorReport extends AbstractMonitorReport {

    public CommitMonitorReport(long domain, TrafficEntity trafficEntity) {
        super(domain, trafficEntity);
    }

    @Override
    protected void doMonitor() {
        long countLong = count.get();
        logger.info("commit count in 30s is: {}", countLong);
    }

    public void addOneCount() {
        addCount(1);
    }

    public void addCount(long c) {
        count.addAndGet(c);
    }
}
