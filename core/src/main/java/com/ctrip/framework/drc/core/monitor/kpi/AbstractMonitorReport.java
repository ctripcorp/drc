package com.ctrip.framework.drc.core.monitor.kpi;

import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author limingdong
 * @create 2020/3/14
 */
public abstract class AbstractMonitorReport extends AbstractMonitorResource {

    //hickwall
    protected AtomicLong count = new AtomicLong(0);

    protected AtomicLong size = new AtomicLong(0);

    private ScheduledExecutorService scheduledExecutorService;

    protected DelayMonitorReport delayMonitorReport;

    public AbstractMonitorReport(long domain, TrafficEntity trafficEntity) {
        this.domain = domain;
        this.trafficEntity = trafficEntity;
        this.clusterName = trafficEntity.getClusterName();
    }

    @Override
    protected void doInitialize() throws Exception {
        LifecycleHelper.initializeIfPossible(delayMonitorReport);
        scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(getClass().getSimpleName() + "-" + clusterName);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    doMonitor();

                    long countLong = count.getAndSet(0);
                    long sizeLong = size.getAndSet(0);
                    hickwallReporter.reportTraffic(trafficEntity, sizeLong);
                    hickwallReporter.reportTransaction(trafficEntity, countLong);

                } catch (Exception e) {
                    logger.error("monitor error", e);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    protected void doStart() throws Exception{
        LifecycleHelper.startIfPossible(delayMonitorReport);
    }

    protected void doStop() throws Exception{
        LifecycleHelper.stopIfPossible(delayMonitorReport);
    }

    @Override
    protected void doDispose() throws Exception {
        LifecycleHelper.disposeIfPossible(delayMonitorReport);
        scheduledExecutorService.shutdown();
    }

    public void addOneCount() {
        addCount(1);
    }

    public void addCount(long c) {
        count.addAndGet(c);
    }

    public void addSize(long s) {
        size.addAndGet(s);
    }

    public String getClusterName() {
        return trafficEntity.getClusterName();
    }

    public void setDelayMonitorReport(DelayMonitorReport delayMonitorReport) {
        this.delayMonitorReport = delayMonitorReport;
    }

    protected abstract void doMonitor();
}
