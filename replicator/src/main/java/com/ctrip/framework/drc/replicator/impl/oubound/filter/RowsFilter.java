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

import java.nio.channels.FileChannel;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;
import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class RowsFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

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
        LogEventType eventType = value.getEventType();
        AbstractRowsEvent afterRowsEvent = null;
        AbstractRowsEvent beforeRowsEvent = null;
        try {
            if (LogEventUtils.isRowsEvent(eventType)) {
                switch (value.getEventType()) {
                    case write_rows_event_v2:
                        beforeRowsEvent = new WriteRowsEvent();
                        noRowFiltered = handRowsEvent(value.getFileChannel(), beforeRowsEvent, value);
                        if (!noRowFiltered) {
                            Columns columns = getColumns(beforeRowsEvent, value);
                            afterRowsEvent = new WriteRowsEvent((WriteRowsEvent) beforeRowsEvent, columns);
                        }
                        break;
                    case update_rows_event_v2:
                        beforeRowsEvent = new UpdateRowsEvent();
                        noRowFiltered = handRowsEvent(value.getFileChannel(), beforeRowsEvent, value);
                        if (!noRowFiltered) {
                            Columns columns = getColumns(beforeRowsEvent, value);
                            afterRowsEvent = new UpdateRowsEvent((UpdateRowsEvent) beforeRowsEvent, columns);
                        }
                        break;
                    case delete_rows_event_v2:
                        beforeRowsEvent = new DeleteRowsEvent();
                        noRowFiltered = handRowsEvent(value.getFileChannel(), beforeRowsEvent, value);
                        if (!noRowFiltered) {
                            Columns columns = getColumns(beforeRowsEvent, value);
                            afterRowsEvent = new DeleteRowsEvent((DeleteRowsEvent) beforeRowsEvent, columns);
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
            beforeRowsEvent.release();
        }
        boolean res = doNext(value, value.isNoRowFiltered());
        if (xid_log_event == eventType) {
            logger.info("[RowsFilter] clear cached result begin: {}", rowsFilterContext.size());
            rowsFilterContext.clear(); // clear filter result
            logger.info("[RowsFilter] clear cached result end: {}", rowsFilterContext.size());
            res = true;
            value.setNoRowFiltered(true);
        }

        return res;

    }

    private boolean handRowsEvent(FileChannel fileChannel, AbstractRowsEvent rowsEvent, OutboundLogEventContext value) throws Exception {
        TableMapLogEvent drcTableMap = loadEvent(fileChannel, rowsEvent, value);
        int beforeSize = rowsEvent.getRows().size();
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMap);
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
                logger.info("[Filter] {} rows of table {} within transaction {} for {}", filterNum, table, value.getGtid(), registryKey);
            }
        }

        return noRowFiltered;
    }

    private TableMapLogEvent loadEvent(FileChannel fileChannel, AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
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

        return drcTableMap;
    }

    private Columns getColumns(AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
        TableMapLogEvent tableMapLogEvent = getTableMapLogEvent(rowsEvent, value);
        TableMapLogEvent drcTableMapLogEvent = getDrcTableMapLogEvent(tableMapLogEvent, value);
        return Columns.from(drcTableMapLogEvent.getColumns());
    }

    private TableMapLogEvent getTableMapLogEvent(AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
        long tableId = rowsEvent.getRowsEventPostHeader().getTableId();
        return value.getTableMapWithinTransaction(tableId);

    }

    private TableMapLogEvent getDrcTableMapLogEvent(TableMapLogEvent tableMapLogEvent, OutboundLogEventContext value) {
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();
        return value.getDrcTableMap(tableName);
    }
}
