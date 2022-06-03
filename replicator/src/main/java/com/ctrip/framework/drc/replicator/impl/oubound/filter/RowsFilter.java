package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.manager.DataMediaManager;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.FileChannel;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;
import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class RowsFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private static final String DRC_INTERNAL_DB = "drcmonitordb";

    protected final Logger ROWS_FILTER_LOGGER = LoggerFactory.getLogger("ROWS FILTER");

    private String registryKey;

    private DataMediaManager dataMediaManager;

    private RowsFilterContext rowsFilterContext = new RowsFilterContext();

    public RowsFilter(DataMediaConfig dataMediaConfig) {
        this.registryKey = dataMediaConfig.getRegistryKey();
        this.dataMediaManager = new DataMediaManager(dataMediaConfig);
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        boolean noRowFiltered = true;
        Pair<Boolean, Columns> pair;
        LogEventType eventType = value.getEventType();
        AbstractRowsEvent afterRowsEvent = null;
        AbstractRowsEvent beforeRowsEvent;
        try {
            if (LogEventUtils.isRowsEvent(eventType)) {
                switch (value.getEventType()) {
                    case write_rows_event_v2:
                        beforeRowsEvent = new WriteRowsEvent();
                        pair = handRowsEvent(value.getFileChannel(), beforeRowsEvent, value);
                        noRowFiltered = pair.getKey();
                        if (!noRowFiltered) {
                            afterRowsEvent = new WriteRowsEvent((WriteRowsEvent) beforeRowsEvent, pair.getValue());
                        }
                        break;
                    case update_rows_event_v2:
                        beforeRowsEvent = new UpdateRowsEvent();
                        pair = handRowsEvent(value.getFileChannel(), beforeRowsEvent, value);
                        noRowFiltered = pair.getKey();
                        if (!noRowFiltered) {
                            afterRowsEvent = new UpdateRowsEvent((UpdateRowsEvent) beforeRowsEvent, pair.getValue());
                        }
                        break;
                    case delete_rows_event_v2:
                        beforeRowsEvent = new DeleteRowsEvent();
                        pair = handRowsEvent(value.getFileChannel(), beforeRowsEvent, value);
                        noRowFiltered = pair.getKey();
                        if (!noRowFiltered) {
                            afterRowsEvent = new DeleteRowsEvent((DeleteRowsEvent) beforeRowsEvent, pair.getValue());
                        }
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("[RowsFilter] error", e);
            value.setCause(e);
        }
        value.setNoRowFiltered(noRowFiltered);
        if (!noRowFiltered) {
            value.setRowsEvent(afterRowsEvent);
        }
        boolean res = doNext(value, value.isNoRowFiltered());
        if (xid_log_event == eventType) {
            rowsFilterContext.clear(); // clear filter result
            res = true;
            value.setNoRowFiltered(true);
        }

        return res;

    }

    private Pair<Boolean, Columns> handRowsEvent(FileChannel fileChannel, AbstractRowsEvent rowsEvent, OutboundLogEventContext value) throws Exception {
        Pair<TableMapLogEvent, Columns> pair = loadEvent(fileChannel, rowsEvent, value);
        int beforeSize = rowsEvent.getRows().size();
        TableMapLogEvent drcTableMap = pair.getKey();
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMap);

        String schemaName = rowsFilterContext.getDrcTableMapLogEvent().getSchemaName();
        if (DRC_INTERNAL_DB.equalsIgnoreCase(schemaName)) {
            return Pair.from(true, pair.getValue());
        }

        RowsFilterResult<List<AbstractRowsEvent.Row>> rowsFilterResult = dataMediaManager.filterRows(rowsEvent, rowsFilterContext);
        boolean noRowFiltered = rowsFilterResult.isNoRowFiltered();

        if (!noRowFiltered) {
            int afterSize;
            List<AbstractRowsEvent.Row> rows = rowsFilterResult.getRes();
            if (rows != null) {
                rowsEvent.setRows(rows);
                afterSize = rows.size();
                int filterNum = beforeSize - afterSize;
                String table = drcTableMap.getSchemaNameDotTableName();
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.rows.filter.event", table);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.rows.filter.row", table, filterNum);
                ROWS_FILTER_LOGGER.info("[Filter] {} rows of table {} within transaction {} for {}", filterNum, table, value.getGtid(), registryKey);
            }
        }

        return Pair.from(noRowFiltered, pair.getValue());
    }

    private Pair<TableMapLogEvent, Columns> loadEvent(FileChannel fileChannel, AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
        // load header
        value.backToHeader();
        EventReader.readEvent(fileChannel, rowsEvent);
        value.restorePosition();
        rowsEvent.loadPostHeader();

        TableMapLogEvent tableMapLogEvent = getTableMapLogEvent(rowsEvent, value);
        TableMapLogEvent drcTableMap = getDrcTableMapLogEvent(tableMapLogEvent, value);

        // load payload
        Columns originColumns = Columns.from(tableMapLogEvent.getColumns());
        Columns columns = Columns.from(drcTableMap.getColumns());
        transformMetaAndType(originColumns, columns);
        rowsEvent.load(columns);

        rowsEvent.release();
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
