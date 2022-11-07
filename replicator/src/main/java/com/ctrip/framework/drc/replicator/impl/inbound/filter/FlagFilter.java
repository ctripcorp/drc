package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.BLACK_TABLE_NAME_F;

/**
 * Created by jixinwang on 2022/10/28
 */
public class FlagFilter extends AbstractLogEventFilter<InboundLogEventContext> {

    @Override
    public boolean doFilter(InboundLogEventContext value) {
        LogEvent logEvent = value.getLogEvent();
        final LogEventType logEventType = logEvent.getLogEventType();

        if (table_map_log_event == logEventType) {
            value.unmark(BLACK_TABLE_NAME_F);
            return doNext(value, value.isInExcludeGroup());
        }

        // guarantee that every transaction will go through UuidFilter or TransactionTableFilter reset
        return doNext(value, false);
    }
}
