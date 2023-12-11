package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionTableMarkedTableMapLogEvent;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.TRANSACTION_TABLE_F;

/**
 * Created by jixinwang on 2022/2/18
 */
public class TransactionTableFilter extends AbstractLogEventFilter<InboundLogEventContext> {

    @Override
    public boolean doFilter(InboundLogEventContext value) {
        LogEvent logEvent = value.getLogEvent();
        final LogEventType logEventType = logEvent.getLogEventType();

        if (LogEventUtils.isGtidLogEvent(logEventType)) {
            // every transaction needs to be reset here or in UuidFilter
            value.reset();
        } else if (table_map_log_event == logEventType) {
            TableMapLogEvent tableMapLogEvent = (TableMapLogEvent) logEvent;
            String schemaName = tableMapLogEvent.getSchemaName();
            String tableName = tableMapLogEvent.getTableName();
            if (DRC_MONITOR_SCHEMA_NAME.equalsIgnoreCase(schemaName)) {
                if (DRC_TRANSACTION_TABLE_NAME.equalsIgnoreCase(tableName) || tableName.toLowerCase().startsWith(DRC_DB_TRANSACTION_TABLE_NAME_PREFIX) || DRC_WRITE_FILTER_TABLE_NAME.equalsIgnoreCase(tableName)) {
                    value.setLogEvent(new TransactionTableMarkedTableMapLogEvent(tableMapLogEvent));
                    value.mark(TRANSACTION_TABLE_F);
                }
            }
        }
        return doNext(value, value.isInExcludeGroup());
    }
}
