package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.manager.DataMediaManager;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;

/**
 * Created by jixinwang on 2022/12/28
 */
public class ExtractFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private Map<String, Columns> filteredColumns = Maps.newHashMap();

    private String registryKey;

    private DataMediaManager dataMediaManager;

    private OutboundMonitorReport outboundMonitorReport;

    public ExtractFilter(DataMediaConfig dataMediaConfig, OutboundMonitorReport outboundMonitorReport) {
        this.registryKey = dataMediaConfig.getRegistryKey();
        this.dataMediaManager = new DataMediaManager(dataMediaConfig);
        this.outboundMonitorReport = outboundMonitorReport;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        value.setFilteredColumnsMap(filteredColumns);

        LogEventType eventType = value.getEventType();
        boolean res = doNext(value, false);
        boolean noRowFiltered = value.isNoRowFiltered();

        if (!noRowFiltered && LogEventUtils.isRowsEvent(eventType)) {
            AbstractRowsEvent afterRowsEvent = null;
            AbstractRowsEvent beforeRowsEvent = null;
            RowsFilterContext rowsFilterContext = value.getRowsFilterContext();
            try {
                beforeRowsEvent = (AbstractRowsEvent) value.getRowsEvent();
                List<TableMapLogEvent.Column> columns = value.getFilteredColumnMap().get(rowsFilterContext.getDrcTableMapLogEvent().getSchemaNameDotTableName());
                afterRowsEvent = beforeRowsEvent.from(columns);
                value.setRowsEvent(afterRowsEvent);
                value.setFilteredEventSize(afterRowsEvent.getLogEventHeader().getEventSize());
            } catch (Exception e) {
                logger.error("[RowsFilter] error", e);
                value.setCause(e);
            } finally {
                if (beforeRowsEvent != null) {
                    beforeRowsEvent.release();  // for extraData used in construct afterRowsEvent
                }
            }
        }

        if (xid_log_event == eventType) {
            res = true;
            value.setNoRowFiltered(true);
        }

        return res;
    }
}
