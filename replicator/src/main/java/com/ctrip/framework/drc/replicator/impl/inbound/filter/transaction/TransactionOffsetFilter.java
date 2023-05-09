package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;

import java.util.List;

/**
 * @Author limingdong
 * @create 2021/10/9
 */
public class TransactionOffsetFilter extends AbstractTransactionFilter {

    @Override
    public boolean doFilter(ITransactionEvent transactionEvent) {
        calculateNextEventStartPosition(transactionEvent);
        return doNext(transactionEvent, false);
    }

    protected void calculateNextEventStartPosition(ITransactionEvent transactionEvent) {
        List<LogEvent> logEvents = transactionEvent.getEvents();

        if (canSkipParseTransaction(transactionEvent)) {
            long nextTransactionStartPosition = 0;
            GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvents.get(0);
            for (int i = 1; i < logEvents.size(); ++i) {
                nextTransactionStartPosition += logEvents.get(i).getLogEventHeader().getEventSize();
            }
            gtidLogEvent.setNextTransactionOffsetAndUpdateEventSize(nextTransactionStartPosition);
        } else {
            for (LogEvent logEvent : logEvents) {
                if (LogEventUtils.isGtidLogEvent(logEvent.getLogEventType())) {
                    GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvent;
                    gtidLogEvent.setNextTransactionOffsetAndUpdateEventSize(0);
                }
            }
        }
    }

}
