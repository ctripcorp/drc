package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class TableFilter extends AbstractPostLogEventFilter<OutboundLogEventContext> {

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        if (table_map_log_event == value.getEventType()) {
            TableMapLogEvent tableMapLogEvent = new TableMapLogEvent();
            EventReader.readEvent(value.getFileChannel(), tableMapLogEvent);
            value.putTableMap(tableMapLogEvent.getTableId(), tableMapLogEvent);
            tableMapLogEvent.release();
            value.setLineFilter(false);
        } else if (xid_log_event == value.getEventType()) {
            value.clearTableMap();
            value.setLineFilter(false);
        }
        return doNext(value, value.isLineFilter());
    }
}
