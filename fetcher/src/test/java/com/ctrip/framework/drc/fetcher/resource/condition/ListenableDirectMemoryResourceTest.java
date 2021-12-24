package com.ctrip.framework.drc.fetcher.resource.condition;

import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent;
import com.ctrip.framework.drc.fetcher.event.MonitoredGtidLogEvent;
import com.ctrip.framework.drc.fetcher.event.MonitoredTableMapEvent;
import com.ctrip.framework.drc.fetcher.event.MonitoredXidEvent;
import com.ctrip.framework.drc.fetcher.event.config.BigTransactionThreshold;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemoryResource.*;

/**
 * @Author limingdong
 * @create 2020/11/26
 */
@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ListenableDirectMemoryResourceTest {

    private ListenableDirectMemoryResource listenableDirectMemoryResource = new ListenableDirectMemoryResource();

    private long Default_Min = ListenableDirectMemoryResource.MIN_MEMORY;

    private LogEventCallBack logEventCallBack;

    private MonitoredGtidLogEvent monitoredGtidLogEvent;

    private LogEventHeader monitoredGtidEventHeader;

    private static final long MonitoredGtidEventSize = 123;

    private MonitoredTableMapEvent monitoredTableMapEvent;

    private LogEventHeader monitoredTableMapEventHeader;

    private static final long MonitoredTableMapEventSize = 456;

    private FetcherRowsEvent monitoredRowsEvent;

    private LogEventHeader monitoredRowsEventHeader;

    private static final long MonitoredRowsEventSize = 789;

    private MonitoredXidEvent monitoredXidEvent;

    private LogEventHeader monitoredXidEventHeader;

    private static final long MonitoredXidEventSize = 369;

    private long TransactionSize = MonitoredGtidEventSize + MonitoredTableMapEventSize + MonitoredRowsEventSize + MonitoredXidEventSize;

    private ExecutorService executorService = ThreadUtils.newFixedThreadPool(20, "ListenableDirectMemoryResourceTest");

    long success = 0;

    long fail = 0;

    @Before
    public void setUp() throws Exception {
        listenableDirectMemoryResource.registryKey = RandomStringUtils.random(1234);
        logEventCallBack = Mockito.mock(LogEventCallBack.class);

        monitoredGtidLogEvent = Mockito.mock(MonitoredGtidLogEvent.class);
        monitoredGtidEventHeader = Mockito.mock(LogEventHeader.class);
        Mockito.when(monitoredGtidLogEvent.getLogEventHeader()).thenReturn(monitoredGtidEventHeader);
        Mockito.when(monitoredGtidEventHeader.getEventSize()).thenReturn(MonitoredGtidEventSize);

        monitoredTableMapEvent = Mockito.mock(MonitoredTableMapEvent.class);
        monitoredTableMapEventHeader = Mockito.mock(LogEventHeader.class);
        Mockito.when(monitoredTableMapEvent.getLogEventHeader()).thenReturn(monitoredTableMapEventHeader);
        Mockito.when(monitoredTableMapEventHeader.getEventSize()).thenReturn(MonitoredTableMapEventSize);

        monitoredRowsEvent = Mockito.mock(FetcherRowsEvent.class);
        monitoredRowsEventHeader = Mockito.mock(LogEventHeader.class);
        Mockito.when(monitoredRowsEvent.getLogEventHeader()).thenReturn(monitoredRowsEventHeader);
        Mockito.when(monitoredRowsEventHeader.getEventSize()).thenReturn(MonitoredRowsEventSize);

        monitoredXidEvent = Mockito.mock(MonitoredXidEvent.class);
        monitoredXidEventHeader = Mockito.mock(LogEventHeader.class);
        Mockito.when(monitoredXidEvent.getLogEventHeader()).thenReturn(monitoredXidEventHeader);
        Mockito.when(monitoredXidEventHeader.getEventSize()).thenReturn(MonitoredXidEventSize);

        LifecycleHelper.initializeIfPossible(listenableDirectMemoryResource);
    }

    @After
    public void tearDown() throws Exception {
        LifecycleHelper.disposeIfPossible(listenableDirectMemoryResource);
    }

    @Test
    public void tryAllocate_01_success() throws InterruptedException {
        MAX_MEMORY = MAX_MEMORY * 10;
        int loop = new Random().nextInt(100);
        CountDownLatch countDownLatch = new CountDownLatch(loop);
        for (int i = 0; i < loop; ++i) {
            CountDownLatch finalCountDownLatch = countDownLatch;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    listenableDirectMemoryResource.tryAllocate(monitoredGtidLogEvent.getLogEventHeader().getEventSize());
                    listenableDirectMemoryResource.tryAllocate(monitoredTableMapEvent.getLogEventHeader().getEventSize());
                    listenableDirectMemoryResource.tryAllocate(monitoredRowsEvent.getLogEventHeader().getEventSize());
                    listenableDirectMemoryResource.tryAllocate(monitoredXidEvent.getLogEventHeader().getEventSize());
                    finalCountDownLatch.countDown();
                }
            });
        }

        boolean finish = countDownLatch.await(2, TimeUnit.SECONDS);
        if (finish) {
            Assert.assertEquals(listenableDirectMemoryResource.get(), loop * TransactionSize);
        }

        listenableDirectMemoryResource.addListener(logEventCallBack);
        Mockito.doNothing().when(logEventCallBack).onSuccess();

        countDownLatch = new CountDownLatch(loop);
        for (int i = 0; i < loop; ++i) {
            CountDownLatch finalCountDownLatch1 = countDownLatch;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    listenableDirectMemoryResource.release(monitoredGtidLogEvent.getLogEventHeader().getEventSize());
                    listenableDirectMemoryResource.release(monitoredTableMapEvent.getLogEventHeader().getEventSize());
                    listenableDirectMemoryResource.release(monitoredRowsEvent.getLogEventHeader().getEventSize());
                    listenableDirectMemoryResource.release(monitoredXidEvent.getLogEventHeader().getEventSize());
                    finalCountDownLatch1.countDown();
                }
            });
        }

        finish = countDownLatch.await(2, TimeUnit.SECONDS);
        if (finish) {
            Assert.assertEquals(listenableDirectMemoryResource.get(), 0);
            Mockito.verify(logEventCallBack, Mockito.atLeast(1)).onSuccess();
        }
    }

    @Test
    public void tryAllocate_02_fail() {
        MAX_MEMORY = Default_Min / 10;
        int l = new Random().nextInt(10);
        long num = MAX_MEMORY / TransactionSize + 1 + l;
        boolean res;
        for (int i = 0; i < num; ++i) {
            res = listenableDirectMemoryResource.tryAllocate(monitoredGtidLogEvent.getLogEventHeader().getEventSize());
            checkRes(res);
            res = listenableDirectMemoryResource.tryAllocate(monitoredTableMapEvent.getLogEventHeader().getEventSize());
            checkRes(res);
            res = listenableDirectMemoryResource.tryAllocate(monitoredRowsEvent.getLogEventHeader().getEventSize());
            checkRes(res);
            res = listenableDirectMemoryResource.tryAllocate(monitoredXidEvent.getLogEventHeader().getEventSize());
            checkRes(res);
        }

        listenableDirectMemoryResource.addListener(logEventCallBack);
        Mockito.doNothing().when(logEventCallBack).onSuccess();
        for (int i = 0; i < num; ++i) {
            listenableDirectMemoryResource.release(monitoredRowsEvent.getLogEventHeader().getEventSize());
        }

        Mockito.verify(logEventCallBack, Mockito.times(1)).onSuccess();

    }

    private void checkRes(boolean res) {
        if (!res) {
            Assert.assertTrue(listenableDirectMemoryResource.get() > MAX_MEMORY);
            fail++;
        } else {
            success++;
        }
    }

    @Test
    public void tryAllocate_03_systemProperty() throws Exception {
        long previousMin = MIN_MEMORY;
        long previousMax = MAX_MEMORY;

        long minSize = new Random().nextLong();
        System.setProperty(MIN_MEMORY_KEY, String.valueOf(minSize));
        long maxSize = new Random().nextLong();
        System.setProperty(MAX_MEMORY_KEY, String.valueOf(maxSize));
        ListenableDirectMemoryResource listenableDirectMemoryResource = new ListenableDirectMemoryResource();
        listenableDirectMemoryResource.registryKey = RandomStringUtils.randomAlphabetic(12);
        listenableDirectMemoryResource.initialize();

        Assert.assertEquals(ListenableDirectMemoryResource.MIN_MEMORY, minSize);
        Assert.assertEquals(ListenableDirectMemoryResource.MAX_MEMORY, maxSize);

        ListenableDirectMemoryResource.MIN_MEMORY = previousMin;
        ListenableDirectMemoryResource.MAX_MEMORY = previousMax;

        listenableDirectMemoryResource.dispose();
    }

    @Test
    public void tryAllocate_04_eventNum() throws Exception {
        long previousMin = MIN_MEMORY;
        long previousMax = MAX_MEMORY;

        long minSize = 1000;
        System.setProperty(MIN_MEMORY_KEY, String.valueOf(minSize));
        long maxSize = 5000;
        System.setProperty(MAX_MEMORY_KEY, String.valueOf(maxSize));
        ListenableDirectMemoryResource listenableDirectMemoryResource = new ListenableDirectMemoryResource();
        listenableDirectMemoryResource.registryKey = RandomStringUtils.randomAlphabetic(12);
        listenableDirectMemoryResource.initialize();

        Assert.assertEquals(ListenableDirectMemoryResource.MIN_MEMORY, minSize);
        Assert.assertEquals(ListenableDirectMemoryResource.MAX_MEMORY, maxSize);

        for (int i = 1; i <= BigTransactionThreshold.getInstance().getThreshold(); ++i) {
             boolean res = listenableDirectMemoryResource.tryAllocate(maxSize + 1);
             if (i == BigTransactionThreshold.getInstance().getThreshold()) {
                Assert.assertFalse(res);
             } else {
                Assert.assertTrue(res);  // event num < ApplierEventGroup.CAPACITY
             }
        }

        for (int i = 1; i <= BigTransactionThreshold.getInstance().getThreshold(); ++i) {
             boolean res = listenableDirectMemoryResource.release(maxSize + 1);  //event num < ApplierEventGroup.CAPACITY
             Assert.assertTrue(res);
        }

        ListenableDirectMemoryResource.MIN_MEMORY = previousMin;
        ListenableDirectMemoryResource.MAX_MEMORY = previousMax;

        listenableDirectMemoryResource.dispose();
    }
}