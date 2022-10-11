package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.ghost.DDLPredication;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.EXCLUDED_DB;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.BLACK_TABLE_NAME_F;

/**
 * @Author limingdong
 * @create 2020/2/24
 */
public class BlackTableNameFilter extends AbstractLogEventFilter<InboundLogEventContext> {

    private HashSet<String> EXCLUDED_TABLE = Sets.newHashSet();

    private HashSet<String> EXCLUDED_CUSTOM_TABLE = Sets.newHashSet();

    private InboundMonitorReport inboundMonitorReport;

    public BlackTableNameFilter(InboundMonitorReport inboundMonitorReport, Set<String> tableNames) {
        this.inboundMonitorReport = inboundMonitorReport;
        EXCLUDED_CUSTOM_TABLE.addAll(tableNames);
    }

    @Override
    public boolean doFilter(InboundLogEventContext value) {

        LogEvent logEvent = value.getLogEvent();
        final LogEventType logEventType = logEvent.getLogEventType();

        if (table_map_log_event == logEventType) {
            TableMapLogEvent tableMapLogEvent = (TableMapLogEvent) logEvent;
            String dbName = tableMapLogEvent.getSchemaName();
            String dbAndTable = tableMapLogEvent.getSchemaNameDotTableName();
            String tableName = tableMapLogEvent.getTableName();
            if (logger.isDebugEnabled()) {
                logger.debug("[Receive] table map log event for {}", dbAndTable);
            }
            inboundMonitorReport.addDb(dbName, value.getGtid());
            inboundMonitorReport.addTable(dbAndTable);

            if (EXCLUDED_DB.contains(dbName) || EXCLUDED_TABLE.contains(tableName) || EXCLUDED_CUSTOM_TABLE.contains(dbAndTable)) {
                value.mark(BLACK_TABLE_NAME_F);
                inboundMonitorReport.addDbFilter(dbAndTable);
            } else if (DDLPredication.isGhostTable(dbAndTable)) {
                value.mark(BLACK_TABLE_NAME_F);
                inboundMonitorReport.addGhostDbFilter(dbAndTable);
            }
        }

        return doNext(value, value.isInExcludeGroup());

    }

    public HashSet<String> getEXCLUDED_CUSTOM_TABLE() {
        return EXCLUDED_CUSTOM_TABLE;
    }

    public Set<String> getEXCLUDED_TABLE() {
        return EXCLUDED_TABLE;
    }
}
