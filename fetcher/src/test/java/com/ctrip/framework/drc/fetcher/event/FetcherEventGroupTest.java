package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.fetcher.event.config.BigTransactionThreshold;
import com.ctrip.framework.drc.fetcher.event.transaction.BaseBeginEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TerminateEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class FetcherEventGroupTest {

    private int BIG_TRANSACTION;

    private long time_to_wait = 250;

    @Before
    public void setUp() {
        try {
            BIG_TRANSACTION = BigTransactionThreshold.getInstance().getThreshold();
        } catch (Throwable e) {
            BIG_TRANSACTION = 1001;
            System.out.println("[setUp] set BIG_TRANSACTION to " + BIG_TRANSACTION);
        }
    }

    @Test
    public void append() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        group.append(new MonitoredGtidLogEvent());
    }

    @Test
    public void appendAndBlock() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        for (int i = 0; i < BIG_TRANSACTION; i++) {
            group.append(new MonitoredGtidLogEvent());
        }
        new Thread(()->{
            try {
                TimeUnit.SECONDS.sleep(1);
                group.next();
            } catch (InterruptedException e) {}
        }).start();
        group.append(new MonitoredGtidLogEvent());
    }

    @Test
    public void next() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup(new MonitoredGtidLogEvent());
        group.next();
    }

    @Test
    public void nextAndBlock() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        new Thread(()->{
            try {
                TimeUnit.SECONDS.sleep(1);
                group.append(new MonitoredGtidLogEvent());
            } catch (InterruptedException e) {}
        }).start();
        group.next();
    }

    @Test
    public void isOverflowed() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        long start = System.currentTimeMillis();
        new Thread(()->{
            try {
                TimeUnit.MILLISECONDS.sleep(time_to_wait);
                for (int i = 0; i < BIG_TRANSACTION; i++) {
                    group.append(new MonitoredGtidLogEvent());
                }
            } catch (InterruptedException e) {}
        }).start();
        assertTrue(group.isOverflowed());
        long diff = System.currentTimeMillis() - start;
        System.out.println(diff);
        assert diff > time_to_wait;
    }

    @Test
    public void isNotOverflowed() throws InterruptedException {
        long SLEEP_TIME = new Random().nextInt(50);
        FetcherEventGroup group = new FetcherEventGroup();
        long start = System.currentTimeMillis();
        new Thread(()->{
            try {
                TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
                group.append(new MonitoredGtidLogEvent());
                group.append(new MonitoredXidEvent());
            } catch (InterruptedException e) {}
        }).start();
        assertFalse(group.isOverflowed());
        long end = System.currentTimeMillis() - start;
        assert end >= SLEEP_TIME;
    }

    @Test
    public void hasNext() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        group.append(new MonitoredGtidLogEvent());
        group.append(new MonitoredXidEvent());
        assertTrue(group.next() instanceof BaseBeginEvent);
        assertTrue(group.next() instanceof TerminateEvent);
        assertNull(group.next());
    }

    @Test
    public void overflowAndContinueToWork() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        new Thread(()->{
            try {
                for (int i = 0; i < BIG_TRANSACTION * 2; i++) {
                    MonitoredGtidLogEvent event = mock(MonitoredGtidLogEvent.class);
                    when(event.getLastCommitted()).thenReturn((long)i);
                    group.append(event);
                }
                group.append(new MonitoredXidEvent());
            } catch (InterruptedException e) {}
        }).start();
        for (int i = 0; i < BIG_TRANSACTION * 2; i++) {
            TransactionEvent event = group.next();
            assert event instanceof MonitoredGtidLogEvent;
            assertEquals(i, ((MonitoredGtidLogEvent) event).getLastCommitted());
        }
        assertTrue(group.next() instanceof MonitoredXidEvent);
        assertNull(group.next());
        assertTrue(group.isOverflowed());
    }

    @Test
    public void reset() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        new Thread(()->{
            try {
                for (int i = 0; i < (BIG_TRANSACTION - 1); i++) {
                    MonitoredGtidLogEvent event = mock(MonitoredGtidLogEvent.class);
                    when(event.getLastCommitted()).thenReturn((long)i);
                    group.append(event);
                }
                group.append(new MonitoredXidEvent());
            } catch (InterruptedException e) {}
        }).start();
        for (int i = 0; i < (BIG_TRANSACTION - 1); i++) {
            TransactionEvent event = group.next();
            assert event instanceof MonitoredGtidLogEvent;
            assertEquals(i, ((MonitoredGtidLogEvent) event).getLastCommitted());
        }
        assertTrue(group.next() instanceof MonitoredXidEvent);
        assertFalse(group.isOverflowed());
        group.reset();
        for (int i = 0; i < (BIG_TRANSACTION - 1); i++) {
            TransactionEvent event = group.next();
            assert event instanceof MonitoredGtidLogEvent;
            assertEquals(i, ((MonitoredGtidLogEvent) event).getLastCommitted());
        }
        assertTrue(group.next() instanceof MonitoredXidEvent);
        assertNull(group.next());
        assertFalse(group.isOverflowed());
    }

    @Test
    public void interruptBeforeNext() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        ArrayBlockingQueue<Throwable> result = new ArrayBlockingQueue<>(1);
        MonitoredGtidLogEvent event = mock(MonitoredGtidLogEvent.class);
        group.append(event);
        Thread t = new Thread(()->{
            Thread.currentThread().interrupt();
            try {
                group.next();
            } catch (Throwable e) {
                result.offer(e);
            }
        });
        t.start();
        t.join();
        assertEquals(InterruptedException.class, result.poll().getClass());
    }

    @Test
    public void interruptBeforeAppend() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        ArrayBlockingQueue<Throwable> result = new ArrayBlockingQueue<>(1);
        Thread t = new Thread(()->{
            Thread.currentThread().interrupt();
            try {
                group.append(new MonitoredGtidLogEvent());
            } catch (Throwable e) {
                result.offer(e);
            }
        });
        t.start();
        t.join();
        assertEquals(InterruptedException.class, result.poll().getClass());
    }

    @Test
    public void interruptBeforeReset() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        ArrayBlockingQueue<Throwable> result = new ArrayBlockingQueue<>(1);
        group.append(new MonitoredGtidLogEvent());
        Thread t = new Thread(()->{
            Thread.currentThread().interrupt();
            try {
                group.reset();
            } catch (Throwable e) {
                result.offer(e);
            }
        });
        t.start();
        t.join();
        assertNull(result.poll());
    }

    @Test
    public void interruptBeforeIsOverflowed() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        ArrayBlockingQueue<Throwable> result = new ArrayBlockingQueue<>(1);
        group.append(new MonitoredGtidLogEvent());
        Thread t = new Thread(()->{
            Thread.currentThread().interrupt();
            try {
                group.isOverflowed();
            } catch (Throwable e) {
                result.offer(e);
            }
        });
        t.start();
        t.join();
        assertEquals(InterruptedException.class, result.poll().getClass());
    }

    @Test
    public void interruptWhenNextAwait() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        Thread t = new Thread(()->{
            try {
                group.next();
            } catch (InterruptedException e) {}
        });
        t.start();
        TimeUnit.MILLISECONDS.sleep(time_to_wait);
        t.interrupt();
        t.join();
    }

    @Test
    public void overflowAndReset() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        for (int i = 0; i < BIG_TRANSACTION; i++) {
            MonitoredGtidLogEvent event = mock(MonitoredGtidLogEvent.class);
            when(event.getLastCommitted()).thenReturn((long)i);
            group.append(event);
        }
        long start = System.currentTimeMillis();
        long cost;
        new Thread(()->{
            try {
                TimeUnit.MILLISECONDS.sleep(time_to_wait);
                for (int i = BIG_TRANSACTION; i < BIG_TRANSACTION * 2; i++) {
                    MonitoredGtidLogEvent event = mock(MonitoredGtidLogEvent.class);
                    when(event.getLastCommitted()).thenReturn((long)i);
                    group.append(event);
                }
                TimeUnit.MILLISECONDS.sleep(time_to_wait);
                group.append(new MonitoredXidEvent());
            } catch (InterruptedException e) {}
        }).start();
        assertTrue(group.isOverflowed());
        cost = System.currentTimeMillis() - start;
        System.out.println("cost: " + cost + "ms");
        assert cost < time_to_wait;
        for (int i = 0; i < BIG_TRANSACTION * 2; i++) {
            TransactionEvent event = group.next();
            assertTrue(event instanceof MonitoredGtidLogEvent);
            assertEquals(i, ((MonitoredGtidLogEvent) event).getLastCommitted());
        }
        cost = System.currentTimeMillis() - start;
        System.out.println("cost: " + cost + "ms");
        assert cost > time_to_wait;
        group.reset();
        for (int i = BIG_TRANSACTION; i < BIG_TRANSACTION * 2; i++) {
            TransactionEvent event = group.next();
            assertTrue(event instanceof MonitoredGtidLogEvent);
            assertEquals(i, ((MonitoredGtidLogEvent) event).getLastCommitted());
        }
        group.next();
        cost = System.currentTimeMillis() - start;
        System.out.println("cost: " + cost + "ms");
        assert cost > time_to_wait * 2;
    }

    @Test
    public void appendAndClose() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        for (int i = 0; i < BIG_TRANSACTION; i++) {
            MonitoredGtidLogEvent MonitoredGtidLogEvent = new MonitoredGtidLogEvent();
            MonitoredGtidLogEvent.setDirectMemory(mock(DirectMemory.class));
            MonitoredGtidLogEvent.setLogEventHeader(mock(LogEventHeader.class));
            group.append(MonitoredGtidLogEvent);
        }
        Thread t = new Thread(() -> {
            group.close();
        });
        t.start();
        MonitoredGtidLogEvent MonitoredGtidLogEvent = new MonitoredGtidLogEvent();
        MonitoredGtidLogEvent.setDirectMemory(mock(DirectMemory.class));
        group.append(MonitoredGtidLogEvent);
    }

    @Test
    public void nextAndClose() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        Thread t = new Thread(() -> {
            group.close();
        });
        t.start();
        group.next();
    }

    @Test
    public void bugFixRefCnt020200706() throws InterruptedException {
        FetcherEventGroup group = new FetcherEventGroup();
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(()-> {
            try {
                group.append(new MonitoredGtidLogEvent());
                for (int i = 0; i < (BIG_TRANSACTION - 1); i++) {
                    group.append(DecryptedWriteRowsEvent.mockSimpleEvent());
                }
                group.append(DecryptedWriteRowsEvent.mockSimpleEvent());
                group.append(DecryptedWriteRowsEvent.mockSimpleEvent());
                latch.countDown();
            } catch (InterruptedException e) {}
        }).start();
        group.isOverflowed();
        group.next();
        MonitoredWriteRowsEvent event = (MonitoredWriteRowsEvent) group.next();
        latch.await();
        event.load(DecryptedWriteRowsEvent.mockSimpleColumns());
    }

    @Test
    public void bugFixLeak20200706() throws InterruptedException {
        List<MonitoredWriteRowsEvent> events = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        FetcherEventGroup group = new FetcherEventGroup();
        for (int i = 0; i < BIG_TRANSACTION; i++) {
            MonitoredWriteRowsEvent applierWriteRowsEvent = spy(DecryptedWriteRowsEvent.mockSimpleEvent());
            applierWriteRowsEvent.setDirectMemory(mock(DirectMemory.class));
            applierWriteRowsEvent.setLogEventHeader(mock(LogEventHeader.class));
            events.add(applierWriteRowsEvent);
        }
        new Thread(()->{
            try {
                MonitoredGtidLogEvent MonitoredGtidLogEvent = new MonitoredGtidLogEvent();
                MonitoredGtidLogEvent.setDirectMemory(mock(DirectMemory.class));
                MonitoredGtidLogEvent.setLogEventHeader(mock(LogEventHeader.class));
                group.append(MonitoredGtidLogEvent);
                for (int i = 0; i < events.size(); i++) {
                    group.append(events.get(i));
                }
                MonitoredXidEvent MonitoredXidEvent = new MonitoredXidEvent();
                MonitoredXidEvent.setDirectMemory(mock(DirectMemory.class));
                MonitoredXidEvent.setLogEventHeader(mock(LogEventHeader.class));
                group.append(MonitoredXidEvent);
                latch.countDown();
            } catch (InterruptedException e) {}
        }).start();
        group.isOverflowed();
        for (int i = 0; i < (BIG_TRANSACTION + 2); i++) {
            TransactionEvent event = group.next();
            if (event instanceof MonitoredWriteRowsEvent) {
//                ((MonitoredWriteRowsEvent) event).mustLoad(mock(TransactionContextResource.class));
                ((MonitoredWriteRowsEvent) event).setDirectMemory(mock(DirectMemory.class));
                ((MonitoredWriteRowsEvent) event).setLogEventHeader(mock(LogEventHeader.class));
                event.release();
            }
        }
        latch.await();
        group.close();
        Mockito.verify(events.get(0), atLeastOnce()).release();
    }
}
