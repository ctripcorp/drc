package com.ctrip.framework.drc.replicator.impl.oubound.filter.context;

import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;

/**
 * @author yongnian
 */
public interface MonitorContext {
    OutboundMonitorReport getOutboundMonitorReport();

    String getSrcRegion();

    String getDstRegion();
}
