package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.google.common.collect.Maps;

import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.GTID_LOGGER;

/**
 * gtid、query、tablemap1、tablemap2、rows1、rows2、xid
 * <p>
 * Created by jixinwang on 2023/10/11
 */
public abstract class TableNameFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private final Map<Long, TableMapLogEvent> skipRowsRelatedTableMap = Maps.newHashMap();

    private LogEventType lastEventType;

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();

        if (table_map_log_event == eventType) {
            filterTableMapEvent(value);
        } else if (drc_table_map_log_event == eventType) {
            filterDrcTableMapEvent(value);
        } else if (LogEventUtils.isRowsEvent(eventType)) {
            filterRowsEvent(value);
        } else if (xid_log_event == value.getEventType()) {
            value.getRowsRelatedTableMap().clear();
            skipRowsRelatedTableMap.clear();
        }

        lastEventType = eventType;
        return doNext(value, value.isSkipEvent());
    }

    protected void filterDrcTableMapEvent(OutboundLogEventContext value) {
        TableMapLogEvent tableMapLogEvent = value.readTableMapEvent();
        if (shouldSkipTableMapEvent(tableMapLogEvent.getSchemaNameDotTableName())) {
            value.setSkipEvent(true);
            GTID_LOGGER.debug("[Skip] drc table map event {} for name filter", tableMapLogEvent.getSchemaNameDotTableName());
        }
    }

    private void filterTableMapEvent(OutboundLogEventContext value) {
        Map<Long, TableMapLogEvent> rowsRelatedTableMap = value.getRowsRelatedTableMap();

        if (isFirstRowsRelatedTableMapEvent()) {
            rowsRelatedTableMap.clear();
            skipRowsRelatedTableMap.clear();
        }

        TableMapLogEvent tableMapLogEvent = value.readTableMapEvent();
        value.setLogEvent(tableMapLogEvent);
        rowsRelatedTableMap.put(tableMapLogEvent.getTableId(), tableMapLogEvent);

        if (shouldSkipTableMapEvent(tableMapLogEvent.getSchemaNameDotTableName())) {
            skipRowsRelatedTableMap.put(tableMapLogEvent.getTableId(), tableMapLogEvent);
            value.setSkipEvent(true);
            GTID_LOGGER.debug("[Skip] table map event {} for name filter", tableMapLogEvent.getSchemaNameDotTableName());
        }
    }

    private boolean isFirstRowsRelatedTableMapEvent() {
        return table_map_log_event != lastEventType;
    }

    protected abstract boolean shouldSkipTableMapEvent(String tableName);

    private boolean isSingleRowsRelatedTableMap(Map<Long, TableMapLogEvent> rowsRelatedTableMap) {
        return rowsRelatedTableMap.size() == 1;
    }

    protected void filterRowsEvent(OutboundLogEventContext value) {
        Map<Long, TableMapLogEvent> rowsRelatedTableMap = value.getRowsRelatedTableMap();
        if (skipRowsRelatedTableMap.isEmpty()) {
            return;
        }

        //trigger has multi rowsRelatedTableMapEvents
        if (isSingleRowsRelatedTableMap(rowsRelatedTableMap)) {
            value.setSkipEvent(true);
        } else {
            AbstractRowsEvent rowsEvent = value.readRowsEvent();
            rowsEvent.loadPostHeader();

            TableMapLogEvent relatedTableMapEvent = skipRowsRelatedTableMap.get(rowsEvent.getRowsEventPostHeader().getTableId());
            if (relatedTableMapEvent != null) {
                value.setSkipEvent(true);
                GTID_LOGGER.info("[Skip] rows event {} for name filter", relatedTableMapEvent);
            }
        }
    }
}
