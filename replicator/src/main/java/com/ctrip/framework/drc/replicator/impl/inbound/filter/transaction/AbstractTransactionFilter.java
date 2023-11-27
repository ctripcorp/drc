package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.server.common.filter.Filter;

import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;

/**
 * @Author limingdong
 * @create 2021/10/9
 */
public abstract class AbstractTransactionFilter extends Filter<ITransactionEvent> {

    protected boolean doNext(ITransactionEvent value, boolean skip) {
        if (!skip && getSuccessor() != null) {
            return getSuccessor().doFilter(value);
        } else {
            return skip;
        }
    }

    //for nextTransactionStartPosition of GtidLogEvent and DdlIndex
    protected boolean canSkipParseTransaction(ITransactionEvent transactionEvent) {
        List<LogEvent> logEvents = transactionEvent.getEvents();

        if (logEvents.isEmpty()) {
            return false;
        }

        LogEvent head = logEvents.get(0);
        if (drc_filter_log_event == head.getLogEventType()) {
            head = logEvents.get(1);
        }

        if (!(head instanceof GtidLogEvent)) {  // with two type
            return false;
        }

        int eventSize = logEvents.size();
        LogEvent tail = logEvents.get(eventSize - 1);
        if (xid_log_event != tail.getLogEventType()) {
            return false;
        }

        if (eventSize > 2) {
            for (int i = 1; i < eventSize - 1; ++i) {
                LogEvent logEvent = logEvents.get(i);
                LogEventType logEventType = logEvent.getLogEventType();
                if (logEventType == drc_ddl_log_event) {
                    transactionEvent.setDdl(true);
                    return false;
                }
            }
        }

        return true;
    }
}
