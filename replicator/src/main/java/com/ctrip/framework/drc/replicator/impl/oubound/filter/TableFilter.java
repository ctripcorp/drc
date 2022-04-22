package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
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

    private Map<Long, TableMapLogEvent> tableMapWithinTransaction = Maps.newHashMap();

    private Map<String, TableMapLogEvent> drcTableMap = Maps.newHashMap();

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();
        FileChannel fileChannel = value.getFileChannel();
        if (table_map_log_event == eventType || drc_table_map_log_event == eventType) {
            TableMapLogEvent tableMapLogEvent = new TableMapLogEvent();
            value.backToHeader();
            EventReader.readEvent(fileChannel, tableMapLogEvent);
            value.setLineFilter(false);
            if (table_map_log_event == eventType) {
                tableMapWithinTransaction.put(tableMapLogEvent.getTableId(), tableMapLogEvent);
                value.setTableMapWithinTransaction(tableMapWithinTransaction);
            } else {
                drcTableMap.put(tableMapLogEvent.getSchemaNameDotTableName(), tableMapLogEvent);
                value.setDrcTableMap(drcTableMap);
            }
        } else if (xid_log_event == eventType) {
            for (TableMapLogEvent tableMapLogEvent : tableMapWithinTransaction.values()) {
                tableMapLogEvent.release();
            }
            this.tableMapWithinTransaction.clear();
            value.setLineFilter(false);
        }

        return doNext(value, value.isLineFilter());
    }
}
