package com.ctrip.framework.drc.core.driver.binlog.constant;

/**
 * Created by @author zhuYongMing on 2019/9/12.
 * see https://dev.mysql.com/doc/internals/en/format-description-event.html
 * see https://dev.mysql.com/doc/internals/en/binlog-event-type.html
 */
public enum LogEventType {

    unknown_log_event(0, 0),

    query_log_event(2, 13),
    stop_log_event(3, 0),
    rotate_log_event(4, 8),

    format_description_log_event(15, 84),
    xid_log_event(16, 0),

    table_map_log_event(19, 0),

    heartbeat_log_event(27, 0),

    rows_query_log_event(29, 0),

    write_rows_event_v2(30, 10),
    update_rows_event_v2(31, 10),
    delete_rows_event_v2(32, 10),
    gtid_log_event(33, 0),

    previous_gtids_log_event(35, 0),
    drc_table_map_log_event(100, 0),
    drc_error_log_event(101, 0),
    drc_gtid_log_event(102, 0),
    drc_heartbeat_log_event(103, 0),
    drc_schema_snapshot_log_event(104, 0),
    drc_ddl_log_event(105, 0),
    drc_delay_monitor_log_event(106, 0),
    drc_index_log_event(107, 0),
    drc_uuid_log_event(108, 0),
    drc_filter_log_event(109, 0);

    public static LogEventType getLogEventType(final int type) {
        switch (type) {
            case 33:
                return gtid_log_event;
            case 102:
                return drc_gtid_log_event;
            case 2:
                return query_log_event;
            case 19:
                return table_map_log_event;
            case 29:
                return rows_query_log_event;
            case 30:
                return write_rows_event_v2;
            case 31:
                return update_rows_event_v2;
            case 32:
                return delete_rows_event_v2;
            case 16:
                return xid_log_event;
            case 3:
                return stop_log_event;
            case 4:
                return rotate_log_event;
            case 15:
                return format_description_log_event;
            case 27:
                return heartbeat_log_event;
            case 35:
                return previous_gtids_log_event;
            case 100:
                return drc_table_map_log_event;
            case 101:
                return drc_error_log_event;
            case 103:
                return drc_heartbeat_log_event;
            case 104:
                return drc_schema_snapshot_log_event;
            case 105:
                return drc_ddl_log_event;
            case 106:
                return drc_delay_monitor_log_event;
            case 107:
                return drc_index_log_event;
            case 108:
                return drc_uuid_log_event;
            case 109:
                return drc_filter_log_event;
            default:
                return unknown_log_event;
        }
    }

    LogEventType(int type, int postHeaderLength) {
        this.type = type;
        this.postHeaderLength = postHeaderLength;
    }

    private int type;
    private int postHeaderLength;

    public int getType() {
        return type;
    }

    public int getPostHeaderLength() {
        return postHeaderLength;
    }
}
