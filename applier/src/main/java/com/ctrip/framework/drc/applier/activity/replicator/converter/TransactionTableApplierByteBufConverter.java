package com.ctrip.framework.drc.applier.activity.replicator.converter;

import com.ctrip.framework.drc.applier.event.*;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.converter.AbstractByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcErrorLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import io.netty.buffer.ByteBuf;

/**
 * Created by jixinwang on 2021/8/20
 */
public class TransactionTableApplierByteBufConverter extends AbstractByteBufConverter {

    @Override
    public LogEvent getNextEmptyLogEvent(ByteBuf byteBuf) {
        final LogEventType nextLogEventType = LogEventUtils.parseNextLogEventType(byteBuf);
        switch (nextLogEventType) {
            case gtid_log_event:
                return new TransactionTableApplierGtidEvent();
            case drc_gtid_log_event:
                return new ApplierDrcGtidEvent();
            case table_map_log_event:
                return new ApplierTableMapEvent();
            case write_rows_event_v2:
                return new ApplierWriteRowsEvent();
            case update_rows_event_v2:
                return new ApplierUpdateRowsEvent();
            case delete_rows_event_v2:
                return new ApplierDeleteRowsEvent();
            case xid_log_event:
                return new ApplierXidEvent();
            case drc_table_map_log_event:
                return new ApplierDrcTableMapEvent();
            case drc_error_log_event:
                return new DrcErrorLogEvent();
            default:
                return null;
        }
    }
}
