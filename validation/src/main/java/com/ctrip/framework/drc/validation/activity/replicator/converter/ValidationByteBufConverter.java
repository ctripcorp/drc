package com.ctrip.framework.drc.validation.activity.replicator.converter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.converter.AbstractByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcErrorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.QueryLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.validation.event.*;
import io.netty.buffer.ByteBuf;

/**
 * @Author Slight
 * Sep 29, 2019
 */
public class ValidationByteBufConverter extends AbstractByteBufConverter {

    @Override
    public LogEvent getNextEmptyLogEvent(ByteBuf byteBuf) {
        final LogEventType nextLogEventType = LogEventUtils.parseNextLogEventType(byteBuf);
        switch (nextLogEventType) {
            case gtid_log_event:
                return new ValidationGtidEvent();
            case query_log_event:
                return new ValidationQueryEvent();
            case table_map_log_event:
                return new ValidationTableMapEvent();
            case write_rows_event_v2:
                return new ValidationWriteRowsEvent();
            case update_rows_event_v2:
                return new ValidationUpdateRowsEvent();
            case delete_rows_event_v2:
                return new ValidationDeleteRowsEvent();
            case xid_log_event:
                return new ValidationXidEvent();
            case drc_table_map_log_event:
                return new ValidationDrcTableMapEvent();
            case drc_error_log_event:
                return new DrcErrorLogEvent();
            default:
                return null;
        }
    }
}

