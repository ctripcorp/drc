package com.ctrip.framework.drc.replicator.impl.inbound.transaction;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.FilterLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionEvent;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObserver;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.TRANSACTION_BUFFER_SIZE;

/**
 * @Author limingdong
 * @create 2020/4/22
 */
public class EventTransactionCache extends AbstractLifecycle implements TransactionCache {

    private static final long INIT_SEQUENCE = -1;

    public static int bufferSize = TRANSACTION_BUFFER_SIZE;

    private int indexMask;

    private LogEvent[] entries;

    private AtomicLong putSequence = new AtomicLong(INIT_SEQUENCE);

    private AtomicLong flushSequence = new AtomicLong(INIT_SEQUENCE);

    private List<Observer> observers = Lists.newCopyOnWriteArrayList();

    private IoCache ioCache;

    private String currentGtid = StringUtils.EMPTY;

    private Filter<ITransactionEvent> filterChain;

    public EventTransactionCache(IoCache ioCache, Filter<ITransactionEvent> filterChain) {
        this.ioCache = ioCache;
        this.filterChain = filterChain;
    }

    protected void doInitialize() {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        indexMask = bufferSize - 1;
        entries = new LogEvent[bufferSize];
    }

    protected void doDispose() {
        reset();
        entries = null;
        currentGtid = StringUtils.EMPTY;
    }

    @Override
    public boolean add(LogEvent logEvent) {
        switch (logEvent.getLogEventType()) {
            case gtid_log_event:
            case drc_gtid_log_event:
                flush();// flush last events
                GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvent;
                put(logEvent);
                currentGtid = gtidLogEvent.getGtid();
                break;
            case xid_log_event:
                put(logEvent);
                flush();
                break;
            default:
                put(logEvent);
                break;
        }

        return true;
    }

    public void reset() {
        long start = this.flushSequence.get() + 1;
        long end = this.putSequence.get();

        if (start <= end) {
            for (long next = start; next <= end; next++) {
                try {
                    this.entries[getIndex(next)].release();
                } catch (Exception e) {
                    logger.error("release entries at {} error", next, e);
                }
            }
        }

        putSequence.set(INIT_SEQUENCE);
        flushSequence.set(INIT_SEQUENCE);
    }

    private void put(LogEvent data) {
        if (checkFreeSlotAt(putSequence.get() + 1)) {
            long current = putSequence.get();
            long next = current + 1;

            entries[getIndex(next)] = data;
            putSequence.set(next);
        } else {
            flush();
            put(data);
        }
    }

    @VisibleForTesting
    @Override
    public void flush() {
        long start = this.flushSequence.get() + 1;
        long end = this.putSequence.get();

        if (start <= end) {
            TransactionEvent transaction = getTransactionEvent();
            transaction.addLogEvent(new FilterLogEvent());
            for (long next = start; next <= end; next++) {
                transaction.addLogEvent(this.entries[getIndex(next)]);
            }
            try {
                filterChain.doFilter(transaction);
                transaction.write(ioCache);
                notifyExecutedGtid();
                flushSequence.set(end);
            } finally {
                transaction.release();
            }
        }
    }

    //in our design, transaction blocked by circular replication needs to be converted into drc_gtid_log_event
    @VisibleForTesting
    public void convertToDrcGtidLogEvent(TransactionEvent transaction) {
        List<LogEvent> logEvents = transaction.getEvents();
        Iterator<LogEvent> iterator = logEvents.iterator();
        while (iterator.hasNext()) {
            LogEvent logEvent = iterator.next();
            LogEventType logEventType = logEvent.getLogEventType();
            if (LogEventType.gtid_log_event == logEventType) {
                GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvent;
                gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
                break;
            }
        }
    }

    private boolean checkFreeSlotAt(final long sequence) {
        final long wrapPoint = sequence - bufferSize;
        if (wrapPoint > flushSequence.get()) {
            return false;
        } else {
            return true;
        }
    }

    private int getIndex(long sequence) {
        return (int) sequence & indexMask;
    }

    @VisibleForTesting
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public void addObserver(Observer observer) {
        if (observer != null && !observers.contains(observer)) {
            observers.add(observer);
        }
    }

    @Override
    public void removeObserver(Observer observer) {
        observers.remove(observer);
    }

    public void notifyExecutedGtid() {
        if (StringUtils.isNotBlank(currentGtid)) {
            for (Observer observer : observers) {
                if (observer instanceof GtidObserver) {
                    observer.update(currentGtid, this);
                }
            }
        }
    }

    protected TransactionEvent getTransactionEvent() {
        return new TransactionEvent();
    }
}
