package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_filter_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.EVENT_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.TRANSACTION_BUFFER_SIZE;

/**
 * @Author limingdong
 * @create 2020/4/23
 */
public class TransactionEvent extends AbstractLogEvent implements ITransactionEvent {

    private List<LogEvent> logEvents = Lists.newArrayList();

    //extra 1 for FilterLogEvent
    private CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer((TRANSACTION_BUFFER_SIZE + 1) * 2);

    private List<ByteBuf> eventByteBufs = new ArrayList<>();

    private boolean isDdl = false;

    private int eventSize = 0;

    private boolean canSkipParseTransaction = false;

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
                compositeByteBuf.addComponent(true, headerBuf);
                compositeByteBuf.addComponent(true, payload);
                eventSize++;
            } else {
                throw new IllegalStateException("haven’t init this event, can’t start write.");
            }
        }

        eventByteBufs.add(compositeByteBuf);
        ioCache.write(eventByteBufs, new TransactionContext(isDdl, eventSize, checkInBigTransaction()));
    }

    private boolean checkInBigTransaction() {
        if (logEvents.isEmpty()) {
            return false;
        }
        LogEvent head = logEvents.get(0);
        if (drc_filter_log_event == head.getLogEventType()) {
            head = logEvents.get(1);
        }
        LogEvent tail = logEvents.get(eventSize - 1);

        if (logEvents.size() >= TRANSACTION_BUFFER_SIZE) {
            return true;
        } else {
            return xid_log_event == tail.getLogEventType() && !LogEventUtils.isGtidLogEvent(head.getLogEventType());
        }
    }

    @Override
    public void release() {
        try {
            compositeByteBuf.release();
            isDdl = false;
            eventSize = 0;
            logEvents.clear();
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
