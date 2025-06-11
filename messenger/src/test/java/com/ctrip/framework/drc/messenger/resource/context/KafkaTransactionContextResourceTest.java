package com.ctrip.framework.drc.messenger.resource.context;

import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.core.mq.Producer;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.messenger.activity.monitor.MqMetricsActivity;
import com.ctrip.framework.drc.messenger.event.ApplierColumnsRelatedTest;
import com.ctrip.framework.drc.messenger.mq.MqProvider;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.framework.drc.core.mq.DcTag.NON_LOCAL;

/**
 * Created by dengquanliang
 * 2025/6/6 15:52
 */
public class KafkaTransactionContextResourceTest implements ApplierColumnsRelatedTest {

    private static final String schema = "prod";
    private static final String table = "hello1";
    private static final String gtid = "6afbad2c-fabe-11e9-878b-fa163eb626bd";

    KafkaTransactionContextResource context;

    @Before
    public void setUp() throws Exception {
        context = new KafkaTransactionContextResource();
        MqMetricsActivity mockMetricsActivity = Mockito.mock(MqMetricsActivity.class);
        context.mqMetricsActivity = mockMetricsActivity;

        context.updateDcTag(NON_LOCAL);
        context.setTableKey(TableKey.from(schema, table));
        context.updateGtid(gtid);
        context.begin();


        LogEventHeader logEventHeader = Mockito.mock(LogEventHeader.class);
        context.setLogEventHeader(logEventHeader);
        Mockito.when(logEventHeader.getEventTimestamp()).thenReturn(1L);

        context.doInitialize();
    }

    @After
    public void tearDown() throws Exception {

    }

    protected  <T extends Object> ArrayList<T> buildArray(T... items) {
        return Lists.newArrayList(items);
    }

    @Test
    public void testSend() throws Exception {
        MqProvider mockProvider = Mockito.mock(MqProvider.class);
        context.mqProvider = mockProvider;
        context.registryKey = "registryKey";
        Mockito.when(mockProvider.getProducers(Mockito.anyString())).thenReturn(Lists.newArrayList(new TestProducer(true)));

        context.insert(buildArray(buildArray(1, "Phi", "2019-12-09 15:00:01.000")),
                Bitmap.from(true, true, true),
                columns0());

        TransactionData.ApplyResult result = context.complete();
        Assert.assertEquals(result, TransactionData.ApplyResult.SUCCESS);
    }

    @Test
    public void testSendDeregister() throws Exception {
        MqProvider mockProvider = Mockito.mock(MqProvider.class);
        context.mqProvider = mockProvider;
        context.registryKey = "registryKey";
        Mockito.when(mockProvider.getProducers(Mockito.anyString())).thenReturn(Lists.newArrayList(new TestProducer(false)));

        context.insert(buildArray(buildArray(1, "Phi", "2019-12-09 15:00:01.000")),
                Bitmap.from(true, true, true),
                columns0());

        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                context.disposeResource();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        TransactionData.ApplyResult result = context.complete();
        Assert.assertEquals(result, TransactionData.ApplyResult.SUCCESS);
    }

    class TestProducer implements Producer {

        private boolean deDeregister;

        public TestProducer(boolean deDeregister) {
            this.deDeregister = deDeregister;
        }

        @Override
        public String getTopic() {
            return "mockTopic";
        }

        @Override
        public boolean sendQmq(List<EventData> eventDatas, EventType eventType) {
            return true;
        }

        @Override
        public boolean sendKafka(List<EventData> eventDatas, EventType eventType, Pair<Phaser, AtomicInteger> phaserAndCounter) {
            Phaser phaser = phaserAndCounter.getKey();
            AtomicInteger value = phaserAndCounter.getValue();

            for (int i = 0; i < eventDatas.size(); i++) {
                phaser.register();
                value.getAndIncrement();

                new Thread(() -> {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    value.decrementAndGet();
                    if (deDeregister) {
                        phaser.arriveAndDeregister();
                    }

                }).start();
            }
            return true;
        }

        @Override
        public void destroy() {

        }
    }
}
