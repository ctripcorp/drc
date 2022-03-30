package com.ctrip.framework.drc.replicator.impl.inbound.converter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.converter.AbstractByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorTableMapLogEvent;
import io.netty.buffer.ByteBuf;

/**
 * Created by @author zhuYongMing on 2019/9/29.
 */
public class ReplicatorByteBufConverter extends AbstractByteBufConverter {

    @Override
    public LogEvent getNextEmptyLogEvent(final ByteBuf byteBuf) {
        final LogEventType nextLogEventType = LogEventUtils.parseNextLogEventType(byteBuf);
        LogEvent logEvent;
        switch (nextLogEventType) {
            case gtid_log_event:
            case drc_gtid_log_event:
                logEvent = new GtidLogEvent();
                break;
            case query_log_event:
                logEvent = new QueryLogEvent();
                break;
            case stop_log_event:
                logEvent = new StopLogEvent();
                break;
            case rotate_log_event:
                logEvent = new RotateLogEvent();
                break;
            case format_description_log_event:
                logEvent = new FormatDescriptionLogEvent();
                break;
            case xid_log_event:
                logEvent = new XidLogEvent();
                break;
            case table_map_log_event:
                logEvent = new ReplicatorTableMapLogEvent();
                break;
            case heartbeat_log_event:
                logEvent = new HeartBeatLogEvent();
                break;
            case rows_query_log_event:
                logEvent = new RowsQueryLogEvent();
                break;
            case write_rows_event_v2:
                logEvent = new WriteRowsEvent();
                break;
            case update_rows_event_v2:
                logEvent = new UpdateRowsEvent();
                break;
            case delete_rows_event_v2:
                logEvent = new DeleteRowsEvent();
                break;
            case previous_gtids_log_event:
                logEvent = new PreviousGtidsLogEvent();
                break;
            case drc_schema_snapshot_log_event:
                logEvent = new DrcSchemaSnapshotLogEvent();
                break;
            case drc_uuid_log_event:
                logEvent = new DrcUuidLogEvent();
                break;
            case drc_error_log_event:
                logEvent =  new DrcErrorLogEvent();
                return logEvent;
            case drc_ddl_log_event:
                logEvent =  new DrcDdlLogEvent();
                return logEvent;
            case drc_heartbeat_log_event:
                logEvent =  new DrcHeartbeatLogEvent();
                return logEvent;
            default:
                return null;
        }

        return logEvent;
    }
}
