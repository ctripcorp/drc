package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DeleteRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.WriteRowsEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;

import java.nio.channels.FileChannel;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class LineFilter extends AbstractPostLogEventFilter<OutboundLogEventContext> {

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        boolean lineFilter = false;
        if (LogEventUtils.isRowsEvent(value.getEventType())) {
            switch (value.getEventType()) {
                case write_rows_event_v2:
                    lineFilter = handLineFilterRowsEvent(value.getFileChannel(), new WriteRowsEvent(), value);
                    break;
                case update_rows_event_v2:
                    lineFilter = handLineFilterRowsEvent(value.getFileChannel(), new UpdateRowsEvent(), value);
                    break;
                case delete_rows_event_v2:
                    lineFilter = handLineFilterRowsEvent(value.getFileChannel(), new DeleteRowsEvent(), value);
                    break;
            }
        }
        value.setLineFilter(lineFilter);
        return doNext(value, value.isLineFilter());
    }

    private boolean handLineFilterRowsEvent(FileChannel fileChannel, AbstractRowsEvent rowsEvent, OutboundLogEventContext value) {
        // check line filter and construct rows event
        if (true) {
//            value.setRowsEvent();
            return true;
        } else {
            return false;
        }
    }
}
