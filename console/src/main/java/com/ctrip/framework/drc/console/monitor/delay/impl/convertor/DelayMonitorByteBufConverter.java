package com.ctrip.framework.drc.console.monitor.delay.impl.convertor;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.converter.AbstractByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import io.netty.buffer.ByteBuf;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_DELAY_MONITOR_LOGGER;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-02
 * convert the byteBuf to timestamp
 */
public class DelayMonitorByteBufConverter extends AbstractByteBufConverter {

    @Override
    public LogEvent getNextEmptyLogEvent(ByteBuf byteBuf) {
        final LogEventType nextLogEventType = LogEventUtils.parseNextLogEventType(byteBuf);
        CONSOLE_DELAY_MONITOR_LOGGER.debug("nextLogEventType: {}", nextLogEventType.getType());
        switch (nextLogEventType) {
            case update_rows_event_v2:
                return new UpdateRowsEvent();
            case drc_delay_monitor_log_event:
                return new DelayMonitorLogEvent();
            case drc_error_log_event:
                return new DrcErrorLogEvent();
            case drc_heartbeat_log_event:
                return new DrcHeartbeatLogEvent();
            case drc_ddl_log_event:
                return new ParsedDdlLogEvent();
            default:
                return null;
        }
    }
}
