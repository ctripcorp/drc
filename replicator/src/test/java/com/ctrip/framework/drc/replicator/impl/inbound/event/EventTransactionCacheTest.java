package com.ctrip.framework.drc.replicator.impl.inbound.event;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.store.AbstractEventTest;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Collection;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/23
 */
public class EventTransactionCacheTest extends AbstractEventTest {

    @Mock
    private IoCache ioCache;

    @Mock
    private Filter<ITransactionEvent> filterChain;

    private static final int bufferSize = 64;

    private static final int loop = 40;

    @Before
    public void initMocks() {
        super.initMocks();
        doNothing().when(ioCache).write(any(Collection.class), any(TransactionContext.class));
    }

    @Test
    public void addTransaction() throws Exception {

        EventTransactionCache eventTransactionCache = new EventTransactionCache(ioCache, filterChain);
        eventTransactionCache.setBufferSize(bufferSize);
        eventTransactionCache.initialize();
        eventTransactionCache.start();
        final int transactionSize = 4;

        for (int i = 0; i < transactionSize * loop; i++) {
            eventTransactionCache.add(getGtidLogEvent());
            eventTransactionCache.add(getTableMapLogEvent());
            eventTransactionCache.add(getUpdateRowsEvent());
            eventTransactionCache.add(getXidLogEvent());
        }

        verify(ioCache, times(transactionSize * loop)).write(any(Collection.class), any(TransactionContext.class));
        eventTransactionCache.stop();
        eventTransactionCache.dispose();
    }

    @Test
    public void addWithoutXid() throws Exception {
        final int transactionSize = 3;
        EventTransactionCache eventTransactionCache = new EventTransactionCache(ioCache, filterChain);
        eventTransactionCache.setBufferSize(bufferSize);
        eventTransactionCache.initialize();
        eventTransactionCache.start();

        for (int i = 0; i < transactionSize * loop; i++) {
            eventTransactionCache.add(getGtidLogEvent());
            eventTransactionCache.add(getTableMapLogEvent());
            eventTransactionCache.add(getUpdateRowsEvent());
        }

        verify(ioCache, times(transactionSize * loop - 1)).write(any(Collection.class), any(TransactionContext.class));
        eventTransactionCache.add(getXidLogEvent());
        verify(ioCache, times(transactionSize * loop)).write(any(Collection.class), any(TransactionContext.class));

        eventTransactionCache.stop();
        eventTransactionCache.dispose();
    }

    @Test
    public void addDrcGtidLogEvent() throws Exception {
        final int transactionSize = 1;
        EventTransactionCache eventTransactionCache = new EventTransactionCache(ioCache, filterChain);
        eventTransactionCache.setBufferSize(bufferSize);
        eventTransactionCache.initialize();
        eventTransactionCache.start();

        for (int i = 0; i < transactionSize * loop; i++) {
            eventTransactionCache.add(getDrcGtidLogEvent());
        }

        verify(ioCache, times(transactionSize * loop - 1)).write(any(Collection.class), any(TransactionContext.class));  // not flush after put drc_gtid_log_event

        eventTransactionCache.stop();
        eventTransactionCache.dispose();
    }

    @Test
    public void addMixedDrcGtidLogEvent() throws Exception {
        final int transactionSize = 4;
        EventTransactionCache eventTransactionCache = new EventTransactionCache(ioCache, filterChain);
        eventTransactionCache.setBufferSize(bufferSize);
        eventTransactionCache.initialize();
        eventTransactionCache.start();

        for (int i = 0; i < transactionSize * loop; i++) {
            eventTransactionCache.add(getGtidLogEvent());
            eventTransactionCache.add(getTableMapLogEvent());
            eventTransactionCache.add(getUpdateRowsEvent());
            eventTransactionCache.add(getXidLogEvent());
            if (i % transactionSize == 0) {
                eventTransactionCache.add(getDrcGtidLogEvent());
            }
        }

        verify(ioCache, times(transactionSize * loop + loop)).write(any(Collection.class), any(TransactionContext.class));

        eventTransactionCache.stop();
        eventTransactionCache.dispose();
    }

    @Test
    public void testDdl() throws Exception {
        final int transactionSize = 4;
        EventTransactionCache eventTransactionCache = new EventTransactionCache(ioCache, filterChain);
        eventTransactionCache.setBufferSize(bufferSize);
        eventTransactionCache.initialize();
        eventTransactionCache.start();

        for (int i = 0; i < transactionSize * loop; i++) {
            eventTransactionCache.add(getGtidLogEvent());
            eventTransactionCache.add(getDrcDdlLogEvent());
            eventTransactionCache.add(getDrcTableMapLogEvent());
            eventTransactionCache.add(getXidLogEvent());
        }

        verify(ioCache, times(transactionSize * loop)).write(any(Collection.class), any(TransactionContext.class)); // true, other are false

        eventTransactionCache.stop();
        eventTransactionCache.dispose();
    }

    @Test
    public void testConvertToDrcGtidLogEvent() {
        EventTransactionCache eventTransactionCache = new EventTransactionCache(ioCache, filterChain);
        TransactionEvent transaction = new TransactionEvent();
        transaction.addLogEvent(getGtidLogEvent());
        transaction.addLogEvent(getDrcTableMapLogEvent());
        transaction.addLogEvent(getXidLogEvent());

        eventTransactionCache.convertToDrcGtidLogEvent(transaction);
        List<LogEvent> logEvents = transaction.getEvents();

        Assert.assertEquals(3, logEvents.size());
        Assert.assertTrue(LogEventType.drc_gtid_log_event == logEvents.get(0).getLogEventType());
    }

    private GtidLogEvent getDrcGtidLogEvent() {
        ByteBuf byteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
        byteBuf.release();
        return gtidLogEvent;
    }

    private GtidLogEvent getGtidLogEvent() {
        ByteBuf byteBuf = getGtidEvent();
        try {
            return new GtidLogEvent().read(byteBuf);
        } finally {
            byteBuf.release();
        }
    }

    private XidLogEvent getXidLogEvent() {
        ByteBuf byteBuf = getXidEvent();
        try {
            return new XidLogEvent().read(byteBuf);
        } finally {
            byteBuf.release();
        }

    }

    private TableMapLogEvent getTableMapLogEvent() {
        ByteBuf byteBuf = getCharsetTypeTableMapEvent();
        try {
            return new TableMapLogEvent().read(byteBuf);
        } finally {
            byteBuf.release();
        }
    }

    private UpdateRowsEvent getUpdateRowsEvent() {
        ByteBuf byteBuf = getMinimalRowsEventByteBuf();
        try {
            return new UpdateRowsEvent().read(byteBuf);
        } finally {
            byteBuf.release();
        }
    }
}
