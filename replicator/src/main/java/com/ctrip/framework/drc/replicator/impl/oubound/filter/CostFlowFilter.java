package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.WriteRowsEvent;
import com.ctrip.framework.drc.core.monitor.entity.CostFlowKey;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;


/**
 * Created by jixinwang on 2022/9/7
 */
public class CostFlowFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private static final long GTID_EVENT_SIZE = 69;

    private String costFlowDb;

    private long transactionSize;

    private OutboundMonitorReport outboundMonitorReport;

    public CostFlowFilter(OutboundMonitorReport outboundMonitorReport) {
        this.outboundMonitorReport = outboundMonitorReport;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        boolean noRowFiltered = doNext(value, value.isNoRowFiltered());

        String dbName = value.getCostFlowKey().getDbName();
        if (dbName != null) {
            costFlowDb = dbName;
        }

        if (noRowFiltered) {
            transactionSize += value.getEventSize();
        } else {
            transactionSize += ((WriteRowsEvent) value.getRowsEvent()).getFilteredEventSize();
        }

        if (xid_log_event == value.getEventType()) {
            CostFlowKey costFlowKey = value.getCostFlowKey();
            costFlowKey.setDbName(costFlowDb);
            outboundMonitorReport.updateCostFlow(value.getCostFlowKey(), transactionSize + GTID_EVENT_SIZE);
            transactionSize = 0;
        }

        return noRowFiltered;
    }
}
