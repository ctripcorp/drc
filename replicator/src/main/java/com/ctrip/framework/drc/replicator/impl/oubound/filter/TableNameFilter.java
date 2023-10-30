package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.google.common.collect.Maps;

import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.GTID_LOGGER;

/**
 * Created by jixinwang on 2023/10/11
 */
public class TableNameFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private Map<Long, TableMapLogEvent> skipRowsRelatedTableMap = Maps.newHashMap();

    private AviatorRegexFilter aviatorFilter;

    private boolean needFilter;

    private LogEventType lastEventType;

    public TableNameFilter(AviatorRegexFilter aviatorFilter) {
        this.aviatorFilter = aviatorFilter;
        this.needFilter = aviatorFilter != null;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();

        if (table_map_log_event == eventType) {
            filterTableMapEvent(value);
        } else if (LogEventUtils.isRowsEvent(eventType)) {
            filterRowsEvent(value);
        } else if (xid_log_event == value.getEventType()) {
            value.getRowsRelatedTableMap().clear();
            skipRowsRelatedTableMap.clear();
        }

        lastEventType = eventType;
        return doNext(value, value.isSkipEvent());
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
            GTID_LOGGER.info("[Skip] table map event {} for name filter", tableMapLogEvent.getSchemaNameDotTableName());
        }
    }

    private boolean isFirstRowsRelatedTableMapEvent() {
        return table_map_log_event != lastEventType;
    }

    private boolean shouldSkipTableMapEvent(String tableName) {
        return needFilter && !aviatorFilter.filter(tableName);
    }

    private boolean isSingleRowsRelatedTableMap(Map<Long, TableMapLogEvent> rowsRelatedTableMap) {
        return rowsRelatedTableMap.size() == 1;
    }

    private void filterRowsEvent(OutboundLogEventContext value) {
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
