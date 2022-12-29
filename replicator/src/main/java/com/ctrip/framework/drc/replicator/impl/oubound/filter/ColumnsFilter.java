package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.manager.DataMediaManager;
import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_table_map_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;

/**
 * Created by jixinwang on 2022/12/15
 */
public class ColumnsFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private String registryKey;

    private DataMediaManager dataMediaManager;

    private OutboundMonitorReport outboundMonitorReport;

    private Map<String ,List<Integer>> columnsIndexMap = Maps.newHashMap();

    public ColumnsFilter(DataMediaConfig dataMediaConfig, OutboundMonitorReport outboundMonitorReport) {
        this.registryKey = dataMediaConfig.getRegistryKey();
        this.dataMediaManager = new DataMediaManager(dataMediaConfig);
        this.outboundMonitorReport = outboundMonitorReport;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();
        if (LogEventUtils.isRowsEvent(eventType)) {
            AbstractRowsEvent rowsEvent = (AbstractRowsEvent) value.getRowsEvent();
            TableMapLogEvent tableMapLogEvent = getTableMapLogEvent(rowsEvent, value);
            String tableName = tableMapLogEvent.getSchemaNameDotTableName();
            TableMapLogEvent drcTableMap = value.getDrcTableMap(tableName);
            List<String> columnsName = drcTableMap.getColumnsName();

            try {
                boolean filtered = dataMediaManager.filterColumns(rowsEvent, tableName, columnsName);
                value.setNoRowFiltered(!filtered);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (table_map_log_event == eventType || drc_table_map_log_event == eventType) {
            TableMapLogEvent tableMapLogEvent = (TableMapLogEvent)value.getRowsEvent();
            boolean needFilter = dataMediaManager.needFilter(tableMapLogEvent.getSchemaNameDotTableName());
            if (needFilter) {
                List<Integer> columnsIndex;
                if (drc_table_map_log_event == eventType) {
                    columnsIndex = dataMediaManager.getColumnsIndex(tableMapLogEvent);
                    columnsIndexMap.put(tableMapLogEvent.getSchemaNameDotTableName(), columnsIndex);

                    Columns columns = Columns.from(tableMapLogEvent.getColumns());
                    List<TableMapLogEvent.Column> columns1 = Lists.newArrayList();
                    for (int index : columnsIndex) {
                        columns1.add(columns.get(index));
                    }

                    Columns newColumns = Columns.from(columns1);

                    Map<String, Columns> filteredColumnMap = value.getFilteredColumnMap();
                    filteredColumnMap.put(tableMapLogEvent.getSchemaNameDotTableName(), newColumns);

                } else {
                    columnsIndex = columnsIndexMap.get(tableMapLogEvent.getSchemaNameDotTableName());
                }

                List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();
                List<TableMapLogEvent.Column> newColumns = Lists.newArrayList();
                for (int columnIndex : columnsIndex) {
                    newColumns.add(columns.get(columnIndex));
                }

                TableMapLogEvent newTableMapLogEvent;
                try {
                    if (drc_table_map_log_event == eventType) {
                        newTableMapLogEvent = new TableMapLogEvent(0, 0, 0, tableMapLogEvent.getSchemaName(), tableMapLogEvent.getTableName(), newColumns, tableMapLogEvent.getIdentifiers());
                    } else {
                        newTableMapLogEvent = new TableMapLogEvent(0, 0, tableMapLogEvent.getTableId(), tableMapLogEvent.getSchemaName(), tableMapLogEvent.getTableName(), newColumns, tableMapLogEvent.getIdentifiers(), table_map_log_event, tableMapLogEvent.getFlags());
                    }

                    value.setRowsEvent(newTableMapLogEvent);
                    value.setNoRowFiltered(false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return value.isNoRowFiltered();
    }

    private TableMapLogEvent getTableMapLogEvent(AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
        long tableId = rowsEvent.getRowsEventPostHeader().getTableId();
        TableMapLogEvent tableMapLogEvent = value.getTableMapWithinTransaction(tableId);
        return tableMapLogEvent;
    }
}
