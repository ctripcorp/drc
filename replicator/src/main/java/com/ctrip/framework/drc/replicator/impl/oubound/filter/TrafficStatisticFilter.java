package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.monitor.entity.TrafficStatisticKey;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;


/**
 * Created by jixinwang on 2022/9/7
 */
public class TrafficStatisticFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private static final long GTID_EVENT_SIZE = 69;

    private String trafficStatisticDb;

    private long transactionSize;

    private OutboundMonitorReport outboundMonitorReport;

    public TrafficStatisticFilter(OutboundMonitorReport outboundMonitorReport) {
        this.outboundMonitorReport = outboundMonitorReport;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        boolean noRowFiltered = doNext(value, value.isNoRowFiltered());

        String dbName = value.getTrafficStatisticKey().getDbName();
        if (dbName != null) {
            trafficStatisticDb = dbName;
            logger.info("[flow] trafficStatisticDb is: {}, dbName is: {}", trafficStatisticDb, dbName);
        }

        transactionSize += value.getFilteredEventSize();

        if (xid_log_event == value.getEventType()) {
            TrafficStatisticKey trafficStatisticKey = value.getTrafficStatisticKey();
            trafficStatisticKey.setDbName(trafficStatisticDb);
            logger.info("[flow] db is: {}", trafficStatisticDb);
            outboundMonitorReport.updateTrafficStatistic(trafficStatisticKey, transactionSize + GTID_EVENT_SIZE);
            transactionSize = 0;
        }

        return noRowFiltered;
    }
}
