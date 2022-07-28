package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_TRANSACTION_TABLE_NAME;

/**
 * Created by jixinwang on 2022/2/18
 */
public class TransactionTableFilter extends AbstractLogEventFilter<InboundLogEventContext> {

    @Override
    public boolean doFilter(InboundLogEventContext value) {
        LogEvent logEvent = value.getLogEvent();
        final LogEventType logEventType = logEvent.getLogEventType();

        if (LogEventUtils.isGtidLogEvent(logEventType)) {
            value.setInExcludeGroup(false);
        } else if (table_map_log_event == logEventType) {
            TableMapLogEvent tableMapLogEvent = (TableMapLogEvent) logEvent;
            String tableName = tableMapLogEvent.getTableName();
            if (DRC_TRANSACTION_TABLE_NAME.equalsIgnoreCase(tableName)) {
                value.setInExcludeGroup(true);
                value.setTransactionTableRelated(true);
            }
        }
        return doNext(value, value.isInExcludeGroup());
    }
}
