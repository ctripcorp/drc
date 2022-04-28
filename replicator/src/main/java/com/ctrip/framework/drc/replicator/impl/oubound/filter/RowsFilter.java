package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.manager.DataMediaManager;

import java.nio.channels.FileChannel;
import java.util.List;

import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class RowsFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private String registryKey;

    private DataMediaManager dataMediaManager;

    public RowsFilter(DataMediaConfig dataMediaConfig) {
        this.registryKey = dataMediaConfig.getRegistryKey();
        this.dataMediaManager = new DataMediaManager(dataMediaConfig);
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        boolean noRowFiltered = true;
        try {
            if (LogEventUtils.isRowsEvent(value.getEventType())) {
                switch (value.getEventType()) {
                    case write_rows_event_v2:
                        noRowFiltered = handRowsEvent(value.getFileChannel(), new WriteRowsEvent(), value);
                        break;
                    case update_rows_event_v2:
                        noRowFiltered = handRowsEvent(value.getFileChannel(), new UpdateRowsEvent(), value);
                        break;
                    case delete_rows_event_v2:
                        noRowFiltered = handRowsEvent(value.getFileChannel(), new DeleteRowsEvent(), value);
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("[RowsFilter] error", e);
            value.setCause(e);
        }
        value.setNoRowFiltered(noRowFiltered);
        return doNext(value, value.isNoRowFiltered());
    }

    private boolean handRowsEvent(FileChannel fileChannel, AbstractRowsEvent rowsEvent, OutboundLogEventContext value) throws Exception {
        TableMapLogEvent drcTableMap = loadEvent(fileChannel, rowsEvent, value);
        int beforeSize = rowsEvent.getRows().size();
        RowsFilterResult<List<List<Object>>> rowsFilterResult = dataMediaManager.filterRows(rowsEvent, drcTableMap);
        boolean noRowFiltered = rowsFilterResult.isNoRowFiltered();

        if (!noRowFiltered) {
            int afterSize;
            List<List<Object>> rows = rowsFilterResult.getRes();
            if (rows != null) {
                afterSize = rows.size();
                int filterNum = beforeSize - afterSize;
                String table = drcTableMap.getSchemaNameDotTableName();
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.rows.filter.event", table);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.rows.filter.row", table, filterNum);
                logger.info("[Filter] {} rows of table {} within transaction {} for {}", filterNum, table, value.getGtid(), registryKey);
            }
            // TODO build event with rows
            value.setRowsEvent(rowsEvent);
        }

        return noRowFiltered;
    }

    private TableMapLogEvent loadEvent(FileChannel fileChannel, AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
        // load header
        value.backToHeader();
        EventReader.readEvent(fileChannel, rowsEvent);
        value.restorePosition();
        rowsEvent.loadPostHeader();

        long tableId = rowsEvent.getRowsEventPostHeader().getTableId();
        TableMapLogEvent tableMapLogEvent = value.getTableMapWithinTransaction(tableId);
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();
        TableMapLogEvent drcTableMap = value.getDrcTableMap(tableName);

        // load payload
        Columns originColumns = Columns.from(tableMapLogEvent.getColumns());
        Columns columns = Columns.from(drcTableMap.getColumns());
        transformMetaAndType(originColumns, columns);
        rowsEvent.load(columns);

        return drcTableMap;
    }
}
