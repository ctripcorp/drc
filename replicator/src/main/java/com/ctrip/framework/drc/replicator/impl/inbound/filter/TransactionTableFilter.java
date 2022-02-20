package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_TRANSACTION_TABLE_NAME;

/**
 * Created by jixinwang on 2022/2/18
 */
public class TransactionTableFilter extends AbstractLogEventFilter {

    @Override
    public boolean doFilter(LogEventWithGroupFlag value) {
        LogEvent logEvent = value.getLogEvent();
        final LogEventType logEventType = logEvent.getLogEventType();

        if (table_map_log_event == logEventType) {
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
