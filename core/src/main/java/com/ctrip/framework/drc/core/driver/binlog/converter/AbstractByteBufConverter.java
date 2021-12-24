package com.ctrip.framework.drc.core.driver.binlog.converter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * Created by @author zhuYongMing on 2019/9/18.
 */
public abstract class AbstractByteBufConverter implements ByteBufConverter {

    private LogEvent unFinishedLogEvent;

    @Override
    public synchronized List<LogEvent> convert(final ByteBuf byteBuf) {
        final List<LogEvent> logEvents = Lists.newArrayList();
        if (unFinishedLogEvent == null) {
            // has'nt unFinished logEvent last time
            while (byteBuf.isReadable()) {
                // byteBuf has remaining elements
                final LogEvent logEvent = getNextEmptyLogEvent(byteBuf);
                if (logEvent == null) {
                    byteBuf.skipBytes(byteBuf.readableBytes());
                    return null;
                }
                if (null == logEvent.read(byteBuf)) {
                    // not enough byteBuf to construct current logEvent
                    unFinishedLogEvent = logEvent;
                } else {
                    // enough
                    logEvents.add(logEvent);
                }
            }

            return logEvents;
        } else {
            // has unFinished logEvent last time
            if (null == unFinishedLogEvent.read(byteBuf)) {
                // not enough again...
                return logEvents;
            } else {
                // enough
                logEvents.add(unFinishedLogEvent);
                unFinishedLogEvent = null;

                while (byteBuf.isReadable()) {
                    // byteBuf has remaining elements
                    final LogEvent logEvent = getNextEmptyLogEvent(byteBuf);
                    if (logEvent == null) {
                        byteBuf.skipBytes(byteBuf.readableBytes());
                        return null;
                    }
                    if (null == logEvent.read(byteBuf)) {
                        // not enough byteBuf to construct current logEvent
                        unFinishedLogEvent = logEvent;
                    } else {
                        // enough
                        logEvents.add(logEvent);
                    }
                }
                return logEvents;
            }
        }
    }

    public abstract LogEvent getNextEmptyLogEvent(final ByteBuf byteBuf);

    @VisibleForTesting
    public boolean hasUnFinishedLogEvent() {
        return unFinishedLogEvent != null;
    }
}
