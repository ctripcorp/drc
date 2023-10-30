package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.gtid_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;

/**
 * Created by jixinwang on 2023/10/25
 */
public class MonitorFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private OutboundMonitorReport outboundMonitorReport;

    long transactionSize;

    @Override
    public boolean doFilter(OutboundLogEventContext value) {

        LogEventType eventType = value.getEventType();

        if (gtid_log_event == eventType) {
            transactionSize = 69;
        } else if (xid_log_event == eventType) {
            transactionSize += 31;

        }

        return false;
    }
}
