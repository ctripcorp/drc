package com.ctrip.framework.drc.monitor.utils;

import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.kpi.AbstractMonitorReport;

/**
 * Created by jixinwang on 2020/12/9
 */
public class ResultCompareMonitorReport extends AbstractMonitorReport {

    public ResultCompareMonitorReport(long domain, TrafficEntity trafficEntity) {
        super(domain, trafficEntity);
    }

    @Override
    protected void doMonitor() {
        long countLong = count.get();
        logger.info(getClass().getSimpleName() + " commit count in 30s: " + countLong);
    }

    public void addOneCount() {
        addCount(1);
    }

    public void addCount(long c) {
        count.addAndGet(c);
    }
}
