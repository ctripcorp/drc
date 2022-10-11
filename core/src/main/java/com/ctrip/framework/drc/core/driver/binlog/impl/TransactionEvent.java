package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.EVENT_LOGGER;

/**
 * @Author limingdong
 * @create 2020/4/23
 */
public class TransactionEvent extends AbstractLogEvent implements ITransactionEvent {

    private List<LogEvent> logEvents = Lists.newArrayList();

    private CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();

    private List<ByteBuf> eventByteBufs = new ArrayList<>();

    private boolean isDdl = false;

    public void addLogEvent(LogEvent logEvent) {
        logEvents.add(logEvent);
    }

    @Override
    public void write(IoCache ioCache) {
        for (LogEvent logEvent : logEvents) {
            LogEventHeader logEventHeader = logEvent.getLogEventHeader();
            ByteBuf headerBuf;

            if (logEventHeader != null) {
                headerBuf = logEventHeader.getHeaderBuf();
            } else {
                throw new IllegalStateException("haven’t init this event, can’t start write.");
            }

            ByteBuf payload = logEvent.getPayloadBuf();
            if (payload != null && headerBuf != null) {
                headerBuf.readerIndex(0);
                payload.readerIndex(0);
                compositeByteBuf.addComponent(true, headerBuf);
                compositeByteBuf.addComponent(true, payload);
            } else {
                throw new IllegalStateException("haven’t init this event, can’t start write.");
            }
        }

        eventByteBufs.add(compositeByteBuf);
        ioCache.write(eventByteBufs, isDdl);
    }

    @Override
    public void release() {
        try {
            compositeByteBuf.release();
        } catch (Exception e) {
            EVENT_LOGGER.error("released compositeByteBuf error", e);
        }
    }

    @Override
    public void setDdl(boolean ddl) {
        this.isDdl = ddl;
    }

    @Override
    public List<LogEvent> getEvents() {
        return logEvents;
    }

    @Override
    public void setEvents(List<LogEvent> logEvents) {
        this.logEvents = logEvents;
    }

    @Override
    public boolean passFilter() {
        return true;
    }

    @VisibleForTesting
    public boolean isDdl() {
        return isDdl;
    }
}
