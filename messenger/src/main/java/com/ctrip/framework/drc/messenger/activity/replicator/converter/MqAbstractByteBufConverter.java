package com.ctrip.framework.drc.messenger.activity.replicator.converter;

import com.ctrip.framework.drc.messenger.event.MessengerDeleteRowsEvent;
import com.ctrip.framework.drc.messenger.event.MessengerUpdateRowsEvent;
import com.ctrip.framework.drc.messenger.event.MessengerWriteRowsEvent;
import com.ctrip.framework.drc.messenger.event.mq.MqApplierDrcTableMapEvent;
import com.ctrip.framework.drc.messenger.event.mq.MqApplierGtidEvent;
import com.ctrip.framework.drc.messenger.event.mq.MqApplierTableMapEvent;
import com.ctrip.framework.drc.messenger.event.mq.MqApplierXidEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.converter.AbstractByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcErrorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcHeartbeatLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.mq.DcTag;
import com.ctrip.framework.drc.fetcher.event.ApplierDrcUuidLogEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierPreviousGtidsLogEvent;
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
                return new MessengerWriteRowsEvent();
            case update_rows_event_v2:
                return new MessengerUpdateRowsEvent();
            case delete_rows_event_v2:
                return new MessengerDeleteRowsEvent();
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
