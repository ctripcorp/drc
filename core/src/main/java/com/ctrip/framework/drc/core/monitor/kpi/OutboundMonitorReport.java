package com.ctrip.framework.drc.core.monitor.kpi;

import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author limingdong
 * @create 2020/3/14
 */
public class OutboundMonitorReport extends AbstractMonitorReport {

    private static final String OUTBOUND_GTID = "DRC.replicator.outbound.gtid";

    private Map<String, AtomicLong> outboundGtid = Maps.newConcurrentMap();

    public OutboundMonitorReport(long domain, TrafficEntity trafficEntity) {
        super(domain, trafficEntity);
    }

    @Override
    protected void doMonitor() {

        for (Map.Entry<String, AtomicLong> entry : outboundGtid.entrySet()) {
            AtomicLong atomicLong = entry.getValue();
            DefaultEventMonitorHolder.getInstance().logEvent(OUTBOUND_GTID, entry.getKey(), atomicLong.getAndSet(0));
        }
    }

    public void addOutboundGtid(String applierName, String gtid) {
        AtomicLong atomicLong = outboundGtid.get(applierName);
        if(atomicLong == null) {
            atomicLong = new AtomicLong(0);
            outboundGtid.put(applierName, atomicLong);
        }
        atomicLong.addAndGet(1);
        delayMonitorReport.sendGtid(gtid);
    }

    public String getClusterName() {
        return trafficEntity.getClusterName();
    }

}
