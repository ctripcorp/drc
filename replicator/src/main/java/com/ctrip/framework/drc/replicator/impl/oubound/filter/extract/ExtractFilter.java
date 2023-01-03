package com.ctrip.framework.drc.replicator.impl.oubound.filter.extract;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.ROWS_FILTER_LOGGER;
import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * Created by jixinwang on 2022/12/28
 */
public class ExtractFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private String registryKey;

    private Filter<ExtractFilterContext> filterChain;

    private ExtractFilterContext extractContext = new ExtractFilterContext();

    public ExtractFilter(OutboundFilterChainContext context) {
        registryKey = context.getDataMediaConfig().getRegistryKey();
        filterChain = new ExtractFilterChainFactory().createFilterChain(ExtractFilterChainContext.from(context));
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();
        AbstractRowsEvent beforeRowsEvent = null;
        try {
            if (LogEventUtils.isRowsEvent(eventType)) {
                switch (eventType) {
                    case write_rows_event_v2:
                        beforeRowsEvent = new FilteredWriteRowsEvent();
                        break;
                    case update_rows_event_v2:
                        beforeRowsEvent = new FilteredUpdateRowsEvent();
                        break;
                    case delete_rows_event_v2:
                        beforeRowsEvent = new FilteredDeleteRowsEvent();
                        break;
                }
                Pair<TableMapLogEvent, Columns> pair = loadEvent(beforeRowsEvent, value);

                TableMapLogEvent drcTableMapLogEvent = pair.getKey();
                String tableName = drcTableMapLogEvent.getSchemaNameDotTableName();

                Columns columns = pair.getValue();
                List<Integer> extractedColumnsIndex = value.getExtractedColumnsIndex(tableName);

                extractContext.setRowsEvent(beforeRowsEvent);
                extractContext.setDrcTableMapLogEvent(drcTableMapLogEvent);
                extractContext.setGtid(value.getGtid());
                extractContext.setExtractedColumnsIndex(extractedColumnsIndex);
                filterChain.doFilter(extractContext);

                boolean shouldRewrite = extractContext.extracted();
                value.setNoRewrite(!shouldRewrite);
                if (shouldRewrite) {
                    AbstractRowsEvent afterRowsEvent;
                    try {
                        Columns extractedColumns = getExtractedColumns(columns, extractedColumnsIndex);
                        afterRowsEvent = beforeRowsEvent.extract(extractedColumns);
                        value.setLogEvent(afterRowsEvent);
                        value.setFilteredEventSize(afterRowsEvent.getLogEventHeader().getEventSize());
                    } catch (Exception e) {
                        logger.error("[ExtractFilter] error", e);
                        value.setCause(e);
                    } finally {
                        if (beforeRowsEvent != null) {
                            beforeRowsEvent.release();  // for extraData used in construct afterRowsEvent
                        }
                    }
                }

                //clear
                extractContext.setRowsExtracted(false);
                extractContext.setColumnsExtracted(false);
            }
        } catch (Exception e) {
            logger.error("[ExtractFilter] error", e);
            value.setCause(e);
        }

        return doNext(value, value.isNoRewrite());
    }

    private Columns getExtractedColumns(Columns columns, List<Integer> extractedColumnsIndex) {
        if (extractContext.getColumnsExtracted()) {
            Columns extractedColumns = new Columns();
            for (int columnIndex : extractedColumnsIndex) {
                extractedColumns.add(columns.get(columnIndex));
            }
            return extractedColumns;
        }
        return columns;
    }

    private Pair<TableMapLogEvent, Columns> loadEvent(AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
        // load header
        value.backToHeader();
        EventReader.readEvent(value.getFileChannel(), rowsEvent);
        value.restorePosition();
        rowsEvent.loadPostHeader();

        TableMapLogEvent tableMapLogEvent = getTableMapLogEvent(rowsEvent, value);
        TableMapLogEvent drcTableMap = getDrcTableMapLogEvent(tableMapLogEvent, value);

        // load payload
        Columns originColumns = Columns.from(tableMapLogEvent.getColumns());
        Columns columns = Columns.from(drcTableMap.getColumns());
        transformMetaAndType(originColumns, columns);
        rowsEvent.load(columns);

        return Pair.from(drcTableMap, columns);
    }

    private TableMapLogEvent getTableMapLogEvent(AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
        long tableId = rowsEvent.getRowsEventPostHeader().getTableId();
        TableMapLogEvent tableMapLogEvent = value.getTableMapWithinTransaction(tableId);
        if (tableMapLogEvent == null) {
            ROWS_FILTER_LOGGER.error("[Filter] error for tableId {} within transaction {} for {}", tableId, value.getGtid(), registryKey);
        }
        return tableMapLogEvent;
    }

    private TableMapLogEvent getDrcTableMapLogEvent(TableMapLogEvent tableMapLogEvent, OutboundLogEventContext value) {
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();
        return value.getDrcTableMap(tableName);
    }
}
