package com.ctrip.framework.drc.replicator.impl.oubound.filter.extract;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.ExtractType;
import com.ctrip.framework.drc.core.server.common.filter.column.ColumnsFilterContext;
import com.ctrip.framework.drc.core.server.manager.DataMediaManager;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.ROWS_FILTER_LOGGER;

/**
 * Created by jixinwang on 2022/12/15
 */
public class ColumnsFilter extends AbstractLogEventFilter<ExtractFilterContext> {

    private String registryKey;

    private DataMediaManager dataMediaManager;

    private OutboundMonitorReport outboundMonitorReport;

    public ColumnsFilter(ExtractFilterChainContext chainContext) {
        this.registryKey = chainContext.getDataMediaConfig().getRegistryKey();
        this.dataMediaManager = new DataMediaManager(chainContext.getDataMediaConfig());
        this.outboundMonitorReport = chainContext.getOutboundMonitorReport();
    }

    @Override
    public boolean doFilter(ExtractFilterContext context) {
        if (context.needExtractColumns()) {
            AbstractRowsEvent rowsEvent = context.getRowsEvent();
            TableMapLogEvent drcTableMapLogEvent = context.getDrcTableMapLogEvent();
            List<Integer> extractedColumnsIndex = context.getExtractedColumnsIndex();

            ColumnsFilterContext columnsFilterContext = new ColumnsFilterContext(drcTableMapLogEvent.getSchemaNameDotTableName(), extractedColumnsIndex);
            boolean columnsExtracted = dataMediaManager.filterColumns(rowsEvent, columnsFilterContext);

            if (columnsExtracted) {
                context.setRewrite(true);
                String schemaName = drcTableMapLogEvent.getSchemaName();
                String tableName = drcTableMapLogEvent.getTableName();
                int rowsSize = rowsEvent.getRows().size();
                int columnsFilterNum = drcTableMapLogEvent.getColumns().size() - extractedColumnsIndex.size();
                ROWS_FILTER_LOGGER.info("[Filter][Column] {} rows {} columns of table {} within transaction {} for {}", rowsSize, columnsFilterNum, tableName, context.getGtid(), registryKey);
                outboundMonitorReport.updateFilteredRows(schemaName, tableName, rowsSize, columnsFilterNum, ExtractType.COLUMN);
            }
        }

        return doNext(context, true);
    }
}
