package com.ctrip.framework.drc.replicator.impl.oubound.filter.extract;

import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.manager.DataMediaManager;

import java.util.List;

import static com.ctrip.framework.drc.core.server.common.filter.ExtractType.ROW;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MONITOR_SCHEMA_NAME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.ROWS_FILTER_LOGGER;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class RowsFilter extends AbstractLogEventFilter<ExtractFilterContext> {

    private String registryKey;

    private DataMediaManager dataMediaManager;

    private OutboundMonitorReport outboundMonitorReport;

    private RowsFilterContext rowsFilterContext;

    public RowsFilter(ExtractFilterChainContext context) {
        this.registryKey = context.getDataMediaConfig().getRegistryKey();
        this.dataMediaManager = new DataMediaManager(context.getDataMediaConfig());
        this.outboundMonitorReport = context.getOutboundMonitorReport();
        this.rowsFilterContext = context.getRowsFilterContext();
    }

    @Override
    public boolean doFilter(ExtractFilterContext context) {
        if (context.needExtractRows()) {
            try {
                AbstractRowsEvent beforeRowsEvent = context.getRowsEvent();
                boolean noRowFiltered = handRowsEvent(beforeRowsEvent, context);
                context.setRewrite(!noRowFiltered);
            } catch (Exception e) {
                logger.error("[RowsFilter] error",e);
                throw new RuntimeException("[RowsFilter] error");
            }
        }

        return doNext(context, context.getRowsEvent().getRows().isEmpty());
    }

    private boolean handRowsEvent(AbstractRowsEvent rowsEvent, ExtractFilterContext context) throws Exception {
        int beforeSize = rowsEvent.getRows().size();
        int afterSize = beforeSize;
        TableMapLogEvent drcTableMap = context.getDrcTableMapLogEvent();
        String table = drcTableMap.getTableName();
        rowsFilterContext.setDrcTableMapLogEvent(drcTableMap);

        String schemaName = rowsFilterContext.getDrcTableMapLogEvent().getSchemaName();
        if (DRC_MONITOR_SCHEMA_NAME.equalsIgnoreCase(schemaName)) {
            return false;
        }

        RowsFilterResult<List<AbstractRowsEvent.Row>> rowsFilterResult = dataMediaManager.filterRows(rowsEvent, rowsFilterContext);
        boolean noRowFiltered = rowsFilterResult.isNoRowFiltered().noRowFiltered();

        if (!noRowFiltered) {
            List<AbstractRowsEvent.Row> rows = rowsFilterResult.getRes();
            if (rows != null) {
                rowsEvent.setRows(rows);
                afterSize = rows.size();
                int filterNum = beforeSize - afterSize;
                ROWS_FILTER_LOGGER.info("[Filter][Row] {}/{} rows of table {}.{} within transaction {} for {}", filterNum, beforeSize, schemaName, table, context.getGtid(), registryKey);
            }
            outboundMonitorReport.updateFilteredRows(schemaName, table, beforeSize, afterSize, ROW);
        }

        return noRowFiltered;
    }
}
