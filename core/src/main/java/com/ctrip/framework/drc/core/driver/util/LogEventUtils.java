package com.ctrip.framework.drc.core.driver.util;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import io.netty.buffer.ByteBuf;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.delete_rows_event_v2;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_table_map_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.write_rows_event_v2;

/**
 * Created by @author zhuYongMing on 2019/9/27.
 */
public class LogEventUtils {

    public static LogEventType parseNextLogEventType(final ByteBuf byteBuf) {
        // no.5 byte is event type of each binlog event
        final int type = byteBuf.getUnsignedByte((byteBuf.readerIndex() + 4));
        return LogEventType.getLogEventType(type);
    }

    public static long parseNextLogEventSize(final ByteBuf byteBuf) {
        // 10-13byte is event size of each binlog event
        return byteBuf.getUnsignedIntLE(byteBuf.readerIndex() + 9);
    }

    public static boolean isOriginGtidLogEvent(LogEventType eventType) {
        return LogEventType.gtid_log_event == eventType;
    }

    public static boolean isDrcGtidLogEvent(LogEventType eventType) {
        return LogEventType.drc_gtid_log_event == eventType;
    }

    public static boolean isDrcTableMapLogEvent(LogEventType eventType) {
        return LogEventType.drc_table_map_log_event == eventType;
    }

    public static boolean isDrcDdlLogEvent(LogEventType eventType) {
        return LogEventType.drc_ddl_log_event == eventType;
    }

    public static boolean isDdlEvent(LogEventType eventType) {
        return isDrcTableMapLogEvent(eventType) || isDrcDdlLogEvent(eventType);
    }

    public static boolean isIndexEvent(LogEventType eventType) {
        return LogEventType.drc_index_log_event == eventType;
    }

    public static boolean isGtidLogEvent(LogEventType eventType) {
        return isOriginGtidLogEvent(eventType) || isDrcGtidLogEvent(eventType);
    }

    public static boolean isSlaveConcerned(LogEventType eventType) {
        return isDdlEvent(eventType) || LogEventType.drc_uuid_log_event == eventType || LogEventType.drc_schema_snapshot_log_event == eventType;
    }

    public static boolean isDrcEvent(LogEventType eventType) {
        return eventType.getType() >= drc_table_map_log_event.getType();
    }

    public static boolean isRowsEvent(LogEventType eventType) {
        return !(eventType.getType() < write_rows_event_v2.getType() || eventType.getType() > delete_rows_event_v2.getType());
    }
}
