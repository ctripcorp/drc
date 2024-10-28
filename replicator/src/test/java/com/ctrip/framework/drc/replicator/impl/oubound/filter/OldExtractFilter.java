package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.manager.DataMediaManager;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.context.ExtractContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilterContext;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.ROWS_FILTER_LOGGER;
import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * Created by jixinwang on 2022/12/28
 */
public class OldExtractFilter extends AbstractLogEventFilter<OldOutboundLogEventContext> {

    private String registryKey;

    private DataMediaManager dataMediaManager;

    private boolean hasRowsFilterConfig;

    private boolean hasColumnsFilterConfig;

    private Map<String, TableMapLogEvent> drcTableMap = Maps.newHashMap();

    private Map<String, List<Integer>> extractedColumnsIndexMap = Maps.newHashMap();

    private Filter<ExtractFilterContext> filterChain;

    private ExtractFilterContext extractContext;

    public OldExtractFilter(ExtractContext context) {
        this.registryKey = context.getDataMediaConfig().getRegistryKey();
        this.hasRowsFilterConfig = context.shouldFilterRows();
        this.hasColumnsFilterConfig = context.shouldFilterColumns();
        this.dataMediaManager = new DataMediaManager(context.getDataMediaConfig());
        this.extractContext = new ExtractFilterContext();
        this.filterChain = new ExtractFilterChainFactory().createFilterChain(ExtractFilterChainContext.from(context, extractContext.getRowsFilterContext()));
    }

    @Override
    public boolean doFilter(OldOutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();

        if (table_map_log_event == eventType || drc_table_map_log_event == eventType) {
            extractTableMapEvent(value, eventType);
        } else if (LogEventUtils.isRowsEvent(eventType)) {
            extractRowsEvent(value);
        } else if (xid_log_event == eventType) {
            extractContext.clear();
        }

        return doNext(value, value.isSkipEvent());
    }

    private void extractTableMapEvent(OldOutboundLogEventContext value, LogEventType eventType) {
        TableMapLogEvent tableMapLogEvent = value.readTableMapEvent();
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();

        if (drc_table_map_log_event == eventType) {
            drcTableMap.put(tableName, tableMapLogEvent);
        }

        if (needExtractColumns(tableName)) {
            TableMapLogEvent rewriteTableMapEvent = rewriteTableMapEvent(tableMapLogEvent, eventType);
            value.setRewrite(true);
            value.setLogEvent(rewriteTableMapEvent);
            value.setEventSize(rewriteTableMapEvent.getLogEventHeader().getEventSize());
        }
    }

    private void extractRowsEvent(OldOutboundLogEventContext value) {
        TableMapLogEvent tableMapEvent = getRowsEventRelatedTableMapEvent(value);
        String tableName = tableMapEvent.getSchemaNameDotTableName();
        boolean needExtractRows = needExtractRows(tableName);
        boolean needExtractColumns = needExtractColumns(tableName);
        if (!needExtractRows && !needExtractColumns) {
            return;
        }

        AbstractRowsEvent rowsEvent = value.readRowsEvent();
        TableMapLogEvent drcTableMapEvent = drcTableMap.get(tableMapEvent.getSchemaNameDotTableName());

        // load payload
        Columns originColumns = Columns.from(tableMapEvent.getColumns());
        Columns columns = Columns.from(drcTableMapEvent.getColumns());
        transformMetaAndType(originColumns, columns);
        rowsEvent.load(columns);

        extractContext.reset(needExtractRows, needExtractColumns, drcTableMapEvent, rowsEvent, value.getGtid(),
                extractedColumnsIndexMap.get(tableName));

        filterChain.doFilter(extractContext);

        if (!extractContext.isRewrite()) {
            return;
        }

        AbstractRowsEvent filteredRowsEvent = extractContext.getRowsEvent();
        if (filteredRowsEvent.getRows().isEmpty()) {
            value.setSkipEvent(true);
        } else {
            try {
                Columns extractedColumns = getColumnsOfRowsEvent(columns, tableName);
                AbstractRowsEvent extractedRowsEvent = filteredRowsEvent.extract(extractedColumns);
                value.setRewrite(true);
                value.setLogEvent(extractedRowsEvent);
                value.setEventSize(extractedRowsEvent.getLogEventHeader().getEventSize());
            } catch (Exception e) {
                logger.error("[ExtractFilter] error", e);
                value.setCause(e);
            }
        }
    }

    private TableMapLogEvent getRowsEventRelatedTableMapEvent(OldOutboundLogEventContext value) {
        Map<Long, TableMapLogEvent> rowsRelatedTableMap = value.getRowsRelatedTableMap();
        TableMapLogEvent rowsEventRelatedTableMapEvent = null;
        if (rowsRelatedTableMap.size() == 1) {
            for (TableMapLogEvent tableMapLogEvent : rowsRelatedTableMap.values()) {
                rowsEventRelatedTableMapEvent = tableMapLogEvent;
                break;
            }
        } else {
            AbstractRowsEvent rowsEvent = value.readRowsEvent();
            rowsEvent.loadPostHeader();
            long tableId = rowsEvent.getRowsEventPostHeader().getTableId();
            rowsEventRelatedTableMapEvent = value.getRowsRelatedTableMap().get(tableId);
            if (rowsEventRelatedTableMapEvent == null) {
                ROWS_FILTER_LOGGER.error("[Filter] error for tableId {} within transaction {} for {}", tableId, value.getGtid(), registryKey);
            }
        }

        return rowsEventRelatedTableMapEvent;
    }

    private Columns getColumnsOfRowsEvent(Columns columns, String tableName) {
        List<Integer> extractedColumnsIndex = extractedColumnsIndexMap.get(tableName);
        if (extractedColumnsIndex == null) {
            return columns;
        } else {
            Columns extractedColumns = new Columns();
            for (int columnIndex : extractedColumnsIndex) {
                extractedColumns.add(columns.get(columnIndex));
            }
            return extractedColumns;
        }
    }

    private TableMapLogEvent rewriteTableMapEvent(TableMapLogEvent tableMapLogEvent, LogEventType eventType) {
        TableMapLogEvent newTableMapLogEvent = null;
        List<TableMapLogEvent.Column> extractedColumns = getExtractedColumns(tableMapLogEvent, eventType);
        try {
            if (drc_table_map_log_event == eventType) {
                List<List<String>> newIdentifiers = extractColumnsIdentifiers(tableMapLogEvent.getIdentifiers(), extractedColumns);
                newTableMapLogEvent = new TableMapLogEvent(0, 0, 0, tableMapLogEvent.getSchemaName(), tableMapLogEvent.getTableName(), extractedColumns, newIdentifiers);
            } else {
                newTableMapLogEvent = new TableMapLogEvent(0, 0, tableMapLogEvent.getTableId(), tableMapLogEvent.getSchemaName(), tableMapLogEvent.getTableName(), extractedColumns, tableMapLogEvent.getIdentifiers(), table_map_log_event, tableMapLogEvent.getFlags());
            }
        } catch (IOException e) {
            logger.error("rewrite table map log event error: ", e);
        }
        return newTableMapLogEvent;
    }

    //TODO: use map to store
    private List<TableMapLogEvent.Column> getExtractedColumns(TableMapLogEvent tableMapLogEvent, LogEventType eventType) {
        List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();
        List<TableMapLogEvent.Column> extractedColumns = Lists.newArrayList();

        List<Integer> extractedColumnsIndex = getExtractedColumnsIndex(tableMapLogEvent, eventType);
        for (int columnIndex : extractedColumnsIndex) {
            extractedColumns.add(columns.get(columnIndex));
        }
        return extractedColumns;
    }

    private List<Integer> getExtractedColumnsIndex(TableMapLogEvent tableMapLogEvent, LogEventType eventType) {
        List<Integer> extractedColumnsIndex;
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();
        if (drc_table_map_log_event == eventType) {
            extractedColumnsIndex = dataMediaManager.getExtractColumnsIndex(tableMapLogEvent);
            if (extractedColumnsIndex.size() == tableMapLogEvent.getColumns().size()) {
                throw new IllegalArgumentException("extract columns error, please check columns filter configuration");
            }
            extractedColumnsIndexMap.put(tableName, extractedColumnsIndex);
        } else {
            extractedColumnsIndex = extractedColumnsIndexMap.get(tableName);
        }
        return extractedColumnsIndex;
    }

    private List<List<String>> extractColumnsIdentifiers(List<List<String>> identifiers, List<TableMapLogEvent.Column> extractedColumns) {
        List<List<String>> newIdentifiers = Lists.newArrayList();
        List<String> extractedColumnsName = Lists.newArrayList();
        for (TableMapLogEvent.Column column : extractedColumns) {
            extractedColumnsName.add(column.getName().toLowerCase());
        }

        for (List<String> identifier : identifiers) {
            List<String> newIdentifier = Lists.newArrayList();
            for (String element : identifier) {
                if (extractedColumnsName.contains(element.toLowerCase())) {
                    newIdentifier.add(element);
                }
            }
            if (!newIdentifier.isEmpty()) {
                newIdentifiers.add(newIdentifier);
            }
        }
        if (newIdentifiers.isEmpty()) {
            throw new IllegalArgumentException("primary or unique key does not exist after extracting columns, please check columns filter configuration");
        }
        return newIdentifiers;
    }

    private boolean needExtractRows(String tableName) {
        return hasRowsFilterConfig && dataMediaManager.hasRowsFilter(tableName);
    }

    private boolean needExtractColumns(String tableName) {
        return hasColumnsFilterConfig && dataMediaManager.hasColumnsFilter(tableName);
    }

    @VisibleForTesting
    public ExtractFilterContext getExtractContext() {
        return extractContext;
    }
}