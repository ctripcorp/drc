package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/9/30
 */
public class TypeConvertFilter extends AbstractTransactionFilter {

    @Override
    public boolean doFilter(ITransactionEvent transactionEvent) {
        List<LogEvent> logEvents = transactionEvent.getEvents();
        if (logEvents != null && !logEvents.isEmpty()) {
            int lastIndex = logEvents.size() - 1;
            if ((logEvents.get(lastIndex) instanceof TransactionTableMarkedXidLogEvent)) {
                for (int i = lastIndex; i >= 0; i--) {
                    LogEvent logEvent = logEvents.get(i);
                    LogEventType logEventType = logEvent.getLogEventType();
                    if (LogEventType.gtid_log_event == logEventType) {
                        GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvent;
                        gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
                        break;
                    }
                }
            }
        }
        return doNext(transactionEvent, false);
    }
}
