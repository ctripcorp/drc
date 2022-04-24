package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;

import java.nio.channels.FileChannel;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class RowsFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private RuleFactory ruleFactory = new DefaultRuleFactory();

    private RowsFilterRule rowsFilterRule;

    public RowsFilter(RowsFilterContext filterContext) {
        rowsFilterRule = ruleFactory.createRowsFilterRule(filterContext);
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        if (LogEventUtils.isRowsEvent(value.getEventType())) {
            switch (value.getEventType()) {
                case write_rows_event_v2:
                    handLineFilterRowsEvent(value.getFileChannel(), new WriteRowsEvent(), value);
                    break;
                case update_rows_event_v2:
                    handLineFilterRowsEvent(value.getFileChannel(), new UpdateRowsEvent(), value);
                    break;
                case delete_rows_event_v2:
                    handLineFilterRowsEvent(value.getFileChannel(), new DeleteRowsEvent(), value);
                    break;
            }
        }
        return doNext(value, value.isNoRowFiltered());
    }

    private boolean handLineFilterRowsEvent(FileChannel fileChannel, AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
        value.backToHeader();
        EventReader.readEvent(fileChannel, rowsEvent);
        rowsEvent.loadPostHeader();
        long tableId = rowsEvent.getRowsEventPostHeader().getTableId();
        TableMapLogEvent tableMapLogEvent = value.getTableMapWithinTransaction(tableId);
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();
        TableMapLogEvent drcTableMap = value.getDrcTableMap(tableName);
        // check line filter and construct rows event
        if (true) {
//            value.setRowsEvent();
            return true;
        } else {
            return false;
        }
    }
}
