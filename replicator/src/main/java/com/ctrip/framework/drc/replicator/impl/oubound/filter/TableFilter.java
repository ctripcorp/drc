package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.manager.DataMediaManager;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;

/**
 * gtid、query、tablemap1、tablemap2、rows1、rows2、xid
 *
 * @Author limingdong
 * @create 2022/4/22
 */
public class TableFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    protected final Logger ROWS_FILTER_LOGGER = LoggerFactory.getLogger("ROWS FILTER");

    private Map<Long, TableMapLogEvent> tableMapWithinTransaction = Maps.newHashMap();  // clear with xid

    private Map<String, TableMapLogEvent> drcTableMap = Maps.newHashMap();  // put every drc_table_map_log_event

    private Map<String, Columns> extractedColumnsMap = Maps.newHashMap();

    private Map<String, List<Integer>> extractedColumnsIndexMap = Maps.newHashMap();

    private DataMediaManager dataMediaManager;

    public TableFilter(DataMediaConfig dataMediaConfig) {
        this.dataMediaManager = new DataMediaManager(dataMediaConfig);
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();
        FileChannel fileChannel = value.getFileChannel();
        if (table_map_log_event == eventType || drc_table_map_log_event == eventType) {
            TableMapLogEvent tableMapLogEvent = new TableMapLogEvent();
            TableMapLogEvent previousTableMapLogEvent;
            value.backToHeader();
            EventReader.readEvent(fileChannel, tableMapLogEvent);

            boolean shouldExtractColumns = shouldExtractColumns(tableMapLogEvent);
            value.setNoRewrite(!shouldExtractColumns);
            if (shouldExtractColumns) {
                TableMapLogEvent rewriteTableMapLogEvent = rewrite(tableMapLogEvent, eventType);
                value.setLogEvent(rewriteTableMapLogEvent);
            }

            if (table_map_log_event == eventType) {
                previousTableMapLogEvent = tableMapWithinTransaction.put(tableMapLogEvent.getTableId(), tableMapLogEvent);
            } else {
                previousTableMapLogEvent = drcTableMap.put(tableMapLogEvent.getSchemaNameDotTableName(), tableMapLogEvent);
            }
            if (previousTableMapLogEvent != null) {
                String tableName = previousTableMapLogEvent.getSchemaNameDotTableName();
                previousTableMapLogEvent.release();
                ROWS_FILTER_LOGGER.info("[Release] TableMapLogEvent for {} of type {}", tableName, previousTableMapLogEvent.getLogEventType());
            }
            value.restorePosition();
        } else if (LogEventUtils.isRowsEvent(eventType)) {
            value.setTableMapWithinTransaction(tableMapWithinTransaction);
            value.setDrcTableMap(drcTableMap);
            value.setExtractedColumnsMap(extractedColumnsMap);
            value.setExtractedColumnsIndexMap(extractedColumnsIndexMap);
        } else {
            value.setNoRewrite(true);
        }

        boolean res = doNext(value, value.isNoRewrite());

        if (xid_log_event == eventType) {
            releaseTableMapEvent();  // clear TableMapLogEvent in transaction
            res = true;
            value.setNoRewrite(true);
        }

        return res;
    }

    private boolean shouldExtractColumns(TableMapLogEvent tableMapLogEvent) {
        return dataMediaManager.hasColumnsFilter(tableMapLogEvent.getSchemaNameDotTableName());
    }

    private TableMapLogEvent rewrite(TableMapLogEvent tableMapLogEvent, LogEventType eventType) {
        TableMapLogEvent newTableMapLogEvent = null;
        List<TableMapLogEvent.Column> extractedColumns = getExtractedColumns(tableMapLogEvent, eventType);
        try {
            if (drc_table_map_log_event == eventType) {
                newTableMapLogEvent = new TableMapLogEvent(0, 0, 0, tableMapLogEvent.getSchemaName(), tableMapLogEvent.getTableName(), extractedColumns, tableMapLogEvent.getIdentifiers());
            } else {
                newTableMapLogEvent = new TableMapLogEvent(0, 0, tableMapLogEvent.getTableId(), tableMapLogEvent.getSchemaName(), tableMapLogEvent.getTableName(), extractedColumns, tableMapLogEvent.getIdentifiers(), table_map_log_event, tableMapLogEvent.getFlags());
            }
        } catch (IOException e) {
            logger.error("rewrite table map log event error: ", e);
        }
        return newTableMapLogEvent;
    }

    private List<TableMapLogEvent.Column> getExtractedColumns(TableMapLogEvent tableMapLogEvent, LogEventType eventType) {
        List<Integer> extractedColumnsIndex;
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();
        if (drc_table_map_log_event == eventType) {
            extractedColumnsIndex = dataMediaManager.getExtractedColumnsIndex(tableMapLogEvent);
            if (extractedColumnsIndex.size() == tableMapLogEvent.getColumns().size()) {
                throw new IllegalArgumentException("extract columns error, please check columns filter configuration");
            }
            extractedColumnsIndexMap.put(tableName, extractedColumnsIndex);

            List<TableMapLogEvent.Column> drcTableMapColumns = tableMapLogEvent.getColumns();
            List<TableMapLogEvent.Column> extractedColumns = Lists.newArrayList();
            for (int extractedIndex : extractedColumnsIndex) {
                extractedColumns.add(drcTableMapColumns.get(extractedIndex));
            }
            Columns rewriteColumns = Columns.from(extractedColumns);
            extractedColumnsMap.put(tableName, rewriteColumns);
        } else {
            extractedColumnsIndex = extractedColumnsIndexMap.get(tableName);
        }

        List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();
        List<TableMapLogEvent.Column> extractedColumns = Lists.newArrayList();
        for (int columnIndex : extractedColumnsIndex) {
            extractedColumns.add(columns.get(columnIndex));
        }
        return extractedColumns;
    }

    @VisibleForTesting
    public Map<Long, TableMapLogEvent> getTableMapWithinTransaction() {
        return tableMapWithinTransaction;
    }

    @VisibleForTesting
    public Map<String, TableMapLogEvent> getDrcTableMap() {
        return drcTableMap;
    }

    @Override
    public void release() {
        releaseTableMapEvent();
        releaseDrcTableMapEvent();
        ROWS_FILTER_LOGGER.info("[Release] TableMapLogEvent within {}", getClass().getSimpleName());
    }

    private void releaseTableMapEvent() {
        try {
            for (TableMapLogEvent tableMapLogEvent : tableMapWithinTransaction.values()) {
                tableMapLogEvent.release();
            }
            this.tableMapWithinTransaction.clear();
        } catch (Exception e) {
        }
    }

    private void releaseDrcTableMapEvent() {
        try {
            for (TableMapLogEvent tableMapLogEvent : drcTableMap.values()) {
                tableMapLogEvent.release();
            }
            this.drcTableMap.clear();
        } catch (Exception e) {
        }
    }
}
