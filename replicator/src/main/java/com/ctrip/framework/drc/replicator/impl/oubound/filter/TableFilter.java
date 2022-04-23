package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.google.common.collect.Maps;

import java.nio.channels.FileChannel;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class TableFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private Map<Long, TableMapLogEvent> tableMapWithinTransaction = Maps.newHashMap();  // clear with xid

    private Map<String, TableMapLogEvent> drcTableMap = Maps.newHashMap();  // put every drc_table_map_log_event

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();
        FileChannel fileChannel = value.getFileChannel();
        if (table_map_log_event == eventType || drc_table_map_log_event == eventType) {
            TableMapLogEvent tableMapLogEvent = new TableMapLogEvent();
            value.backToHeader();
            EventReader.readEvent(fileChannel, tableMapLogEvent);
            value.setSkip(true);
            if (table_map_log_event == eventType) {
                tableMapWithinTransaction.put(tableMapLogEvent.getTableId(), tableMapLogEvent);
            } else {
                drcTableMap.put(tableMapLogEvent.getSchemaNameDotTableName(), tableMapLogEvent);
            }
        } else if (xid_log_event == eventType) {
            for (TableMapLogEvent tableMapLogEvent : tableMapWithinTransaction.values()) {
                tableMapLogEvent.release();
            }
            this.tableMapWithinTransaction.clear();
            value.setSkip(true);
        } else if (LogEventUtils.isRowsEvent(eventType)) {
            value.setTableMapWithinTransaction(tableMapWithinTransaction);
            value.setDrcTableMap(drcTableMap);
        }

        return doNext(value, value.isSkip());
    }
}
