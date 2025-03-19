package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.EVENT_LOGGER;

/**
 * @Author limingdong
 * @create 2020/4/23
 */
public class TransactionEvent extends AbstractLogEvent implements ITransactionEvent {

    private List<LogEvent> logEvents = Lists.newArrayList();

    private final List<ByteBuf> eventByteBufs = Lists.newArrayList();

    private boolean isDdl = false;

    private boolean inBigTransaction = false;

    private boolean canSkipParseTransaction = false;

    public TransactionEvent() {
    }

    public TransactionEvent(boolean inBigTransaction) {
        this.inBigTransaction = inBigTransaction;
    }

    public void addLogEvent(LogEvent logEvent) {
        logEvents.add(logEvent);
    }

    @Override
    public void addFilterLogEvent() {
        logEvents.add(new FilterLogEvent());
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
                eventByteBufs.add(headerBuf);
                eventByteBufs.add(payload);
            } else {
                throw new IllegalStateException("haven’t init this event, can’t start write.");
            }
        }

        ioCache.write(eventByteBufs, new TransactionContext(isDdl, inBigTransaction));
    }

    @Override
    public void release() {
        try {
            isDdl = false;
            inBigTransaction = false;
            logEvents.clear();
            eventByteBufs.forEach(ReferenceCounted::release);
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
    public void setCanSkipParseTransaction(boolean canSkipParseTransaction) {
        this.canSkipParseTransaction = canSkipParseTransaction;
    }

    @Override
    public boolean canSkipParseTransaction() {
        return canSkipParseTransaction;
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
