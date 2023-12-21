package com.ctrip.framework.drc.applier.activity.replicator.converter;

import com.ctrip.framework.drc.applier.event.*;
import com.ctrip.framework.drc.applier.event.mq.MqApplierDrcTableMapEvent;
import com.ctrip.framework.drc.applier.event.mq.MqApplierGtidEvent;
import com.ctrip.framework.drc.applier.event.mq.MqApplierTableMapEvent;
import com.ctrip.framework.drc.applier.event.mq.MqApplierXidEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.converter.AbstractByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcErrorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcHeartbeatLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.mq.DcTag;
import io.netty.buffer.ByteBuf;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqAbstractByteBufConverter extends AbstractByteBufConverter {

    @Override
    public LogEvent getNextEmptyLogEvent(ByteBuf byteBuf) {
        final LogEventType nextLogEventType = LogEventUtils.parseNextLogEventType(byteBuf);
        switch (nextLogEventType) {
            case gtid_log_event:
                return new MqApplierGtidEvent(DcTag.LOCAL);
            case drc_gtid_log_event:
                return new MqApplierGtidEvent(DcTag.NON_LOCAL);
            case table_map_log_event:
                return new MqApplierTableMapEvent();
            case write_rows_event_v2:
                return new ApplierWriteRowsEvent();
            case update_rows_event_v2:
                return new ApplierUpdateRowsEvent();
            case delete_rows_event_v2:
                return new ApplierDeleteRowsEvent();
            case xid_log_event:
                return new MqApplierXidEvent();
            case drc_table_map_log_event:
                return new MqApplierDrcTableMapEvent();
            case drc_error_log_event:
                return new DrcErrorLogEvent();
            case drc_heartbeat_log_event:
                return new DrcHeartbeatLogEvent();
            case drc_uuid_log_event:
                return new ApplierDrcUuidLogEvent();
            case previous_gtids_log_event:
                return new ApplierPreviousGtidsLogEvent();
            default:
                return null;
        }
    }
}
