package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.ReplicatorException;

import java.io.IOException;

/**
 * @author yongnian
 */
public class ScannerPositionFilter extends AbstractPostLogEventFilter<OutboundLogEventContext> {


    public ScannerPositionFilter() {
    }


    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        boolean skipEvent = doNext(value, value.isSkipEvent());

        if (value.getCause() != null) {
            return true;
        }

        if (!value.getFileChannel().isOpen()) {
            return true;
        }

        if (value.getLogEvent() == null) {
            try {
                value.getFileChannel().position(value.getFileChannelPos() + value.getEventSize());
            } catch (IOException e) {
                logger.error("skip position error:", e);
                throw new ReplicatorException(ResultCode.REPLICATOR_SEND_BINLOG_ERROR);
            }
        }

        return skipEvent;
    }
}
