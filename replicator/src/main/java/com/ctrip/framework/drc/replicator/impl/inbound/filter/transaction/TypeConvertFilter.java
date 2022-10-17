package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionTableMarkedTableMapLogEvent;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/9/30
 */
public class TypeConvertFilter extends AbstractTransactionFilter {

    @Override
    public boolean doFilter(ITransactionEvent transactionEvent) {
        List<LogEvent> logEvents = transactionEvent.getEvents();
        LogEvent latestGtidEvent = null;
        for (LogEvent logEvent : logEvents) {
            LogEventType logEventType = logEvent.getLogEventType();
            if (LogEventType.gtid_log_event == logEventType) {
                latestGtidEvent = logEvent;
            } else if (LogEventType.table_map_log_event == logEventType) {
                if (logEvent instanceof TransactionTableMarkedTableMapLogEvent && latestGtidEvent != null) {
                    GtidLogEvent gtidLogEvent = (GtidLogEvent) latestGtidEvent;
                    gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
                }
                break; // break on first table_map_log_event
            }
        }
        return doNext(transactionEvent, false);
    }
}
