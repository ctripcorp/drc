package com.ctrip.framework.drc.messenger.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.core.mq.Producer;
import com.ctrip.framework.drc.messenger.activity.monitor.MqMetricsActivity;
import com.ctrip.framework.drc.messenger.event.ApplierColumnsRelatedTest;
import com.ctrip.framework.drc.messenger.mq.MqProvider;
import com.ctrip.framework.drc.messenger.resource.thread.MqRowEventExecutorResource;
import com.ctrip.framework.drc.messenger.utils.MqDynamicConfig;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.ctrip.framework.drc.core.mq.DcTag.NON_LOCAL;
import static com.ctrip.framework.drc.core.mq.EventType.*;

/**
 * Created by shiruixin
 * 2024/11/8 16:52
 */
public class MqTransactionContextResourceTest implements ApplierColumnsRelatedTest {
    private static final String gtid = "6afbad2c-fabe-11e9-878b-fa163eb626bd";

    private static final String schema = "prod";
    private static final String table = "hello1";

    private MqTransactionContextResource context;

    private MockedStatic<MqDynamicConfig> theMockConfig;

    private List<EventData> finalEventDatas;

    protected  <T extends Object> ArrayList<T> buildArray(T... items) {
        return Lists.newArrayList(items);
    }


    @Before
    public void setUp() throws Exception {
        MqRowEventExecutorResource mqRowEventExecutorResource = new testExecutor();
        mqRowEventExecutorResource.initialize();

        context = new MqTransactionContextResource();
        context.updateDcTag(NON_LOCAL);
        context.setTableKey(TableKey.from(schema, table));
        context.updateGtid(gtid);
        MqProvider mockProvider = Mockito.mock(MqProvider.class);
        context.mqProvider = mockProvider;
        context.registryKey = "registryKey";
        context.applyMode = 2;
        context.mqRowEventExecutor = mqRowEventExecutorResource;


        MqMetricsActivity mockMetricsActivity = Mockito.mock(MqMetricsActivity.class);
        context.mqMetricsActivity = mockMetricsActivity;

        Mockito.when(mockProvider.getProducers(Mockito.anyString())).thenReturn(Lists.newArrayList(new testProducer()));

        MqDynamicConfig mockConfig = Mockito.mock(MqDynamicConfig.class);
        Mockito.when(mockConfig.getBigRowsEventSize()).thenReturn(1);
        theMockConfig = Mockito.mockStatic(MqDynamicConfig.class);
        theMockConfig.when(() -> MqDynamicConfig.getInstance()).thenReturn(mockConfig);
        context.doInitialize();
    }

    @After
    public void tearDown() throws Exception {
        context.doDispose();
        theMockConfig.close();
    }

    @Test
    public void insert() {
        context.insert(buildArray(buildArray(1, "Phi", "2019-12-09 15:00:01.000")),
                Bitmap.from(true, true, true),
                columns0());
        context.complete();
        for (EventData data : finalEventDatas) {
            Assert.assertEquals(schema, data.getSchemaName());
            Assert.assertEquals(table, data.getTableName());
            Assert.assertEquals(INSERT, data.getEventType());
            Assert.assertEquals(NON_LOCAL, data.getDcTag());

            List<EventColumn> afterColumns = data.getAfterColumns();
            Assert.assertEquals(3, afterColumns.size());
            EventColumn column0 = afterColumns.get(0);
            Assert.assertEquals(column0.getColumnValue(),"1");
            EventColumn column1 = afterColumns.get(1);
            Assert.assertEquals(column1.getColumnValue(),"Phi");
            EventColumn column2 = afterColumns.get(2);
            Assert.assertEquals(column2.getColumnValue(),"2019-12-09 15:00:01.000");

            Assert.assertEquals(0, data.getBeforeColumns().size());
        }
    }

    @Test
    public void update() {
        context.update(
                buildArray(buildArray(1, "Mi", "2019-12-09 16:00:00.000")), Bitmap.from(true, true, true),
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")), Bitmap.from(true, true, true),
                columns0());
        context.complete();
        for (EventData data : finalEventDatas) {
            Assert.assertEquals(schema, data.getSchemaName());
            Assert.assertEquals(table, data.getTableName());
            Assert.assertEquals(UPDATE, data.getEventType());
            Assert.assertEquals(NON_LOCAL, data.getDcTag());

            List<EventColumn> beforeColumns = data.getBeforeColumns();
            Assert.assertEquals(3, beforeColumns.size());
            EventColumn beforeColumn0 = beforeColumns.get(0);
            Assert.assertEquals(beforeColumn0.getColumnValue(),"1");
            EventColumn beforeColumn1 = beforeColumns.get(1);
            Assert.assertEquals(beforeColumn1.getColumnValue(),"Mi");
            EventColumn beforeColumn2 = beforeColumns.get(2);
            Assert.assertEquals(beforeColumn2.getColumnValue(),"2019-12-09 16:00:00.000");

            List<EventColumn> afterColumns = data.getAfterColumns();
            Assert.assertEquals(3, afterColumns.size());
            EventColumn afterColumn0 = afterColumns.get(0);
            Assert.assertEquals(afterColumn0.getColumnValue(),"1");
            EventColumn afterColumn1 = afterColumns.get(1);
            Assert.assertEquals(afterColumn1.getColumnValue(),"Phy");
            EventColumn afterColumn2 = afterColumns.get(2);
            Assert.assertEquals(afterColumn2.getColumnValue(),"2019-12-09 16:00:00.001");
        }
    }

    @Test
    public void delete() {
        context.delete(
                buildArray(buildArray(1, "2019-12-09 16:00:00.001")),
                Bitmap.from(true, false, true),
                columns0()
        );
        context.complete();

        for (EventData data : finalEventDatas) {
            Assert.assertEquals(schema, data.getSchemaName());
            Assert.assertEquals(table, data.getTableName());
            Assert.assertEquals(DELETE, data.getEventType());
            Assert.assertEquals(NON_LOCAL, data.getDcTag());

            List<EventColumn> beforeColumns = data.getBeforeColumns();
            Assert.assertEquals(2, beforeColumns.size());
            EventColumn column0 = beforeColumns.get(0);
            Assert.assertEquals(column0.getColumnValue(),"1");
            EventColumn column1 = beforeColumns.get(1);
            Assert.assertEquals(column1.getColumnValue(),"2019-12-09 16:00:00.001");

            Assert.assertEquals(0, data.getAfterColumns().size());
        }
    }

    class testExecutor extends MqRowEventExecutorResource {
        testExecutor() {
            this.registryKey = "dalcluster.mha";
            this.applyMode = 2;
        }
    }

    class testProducer implements Producer {

        @Override
        public String getTopic() {
            return "mockTopic";
        }

        @Override
        public boolean send(List<EventData> eventDatas, EventType eventType) {
            finalEventDatas = eventDatas;
            return true;
        }

        @Override
        public void destroy() {

        }
    }

    @Test
    public void testSendEventDatasNormal() throws Exception {
        MqRowEventExecutorResource executorService = Mockito.mock(MqRowEventExecutorResource.class);
        context.mqRowEventExecutor = executorService;
        Future<Boolean> f = Mockito.mock(Future.class);
        Mockito.when(f.get()).thenReturn(true);
        Mockito.when(executorService.submit(Mockito.any())).thenReturn(f);

        context.doInitialize();
        context.sendEventDatas(buildUpdateEventDatas(), UPDATE);
        context.complete();

        Mockito.verify(executorService, Mockito.times(2)).submit(Mockito.any());
        Mockito.verify(f, Mockito.times(2)).get();
    }

    @Test(expected = RuntimeException.class)
    public void testSendEventDatasWithExecutionException() throws Exception {
        MqRowEventExecutorResource executorService = Mockito.mock(MqRowEventExecutorResource.class);
        context.mqRowEventExecutor = executorService;
        Future<Boolean> f = Mockito.mock(Future.class);
        Mockito.when(f.get()).thenThrow(new ExecutionException("Mocked exception", new Throwable()));
        Mockito.when(executorService.submit(Mockito.any())).thenReturn(f);

        context.sendEventDatas(buildUpdateEventDatas(), UPDATE);
        context.complete();

        Mockito.verify(executorService, Mockito.times(2)).submit(Mockito.any());
    }



    private List<EventData> buildUpdateEventDatas() {
        EventData data = bulidUpdateEvendData("1", "Mike", "Joe");
        EventData data2 = bulidUpdateEvendData("2", "Alice", "John");
        return Lists.newArrayList(data,data2);
    }

    private EventData bulidUpdateEvendData(String idVal, String nameValue1, String nameValue2) {
        EventData data = new EventData();
        data.setSchemaName(schema);
        data.setTableName(table);
        data.setDcTag(NON_LOCAL);
        data.setEventType(UPDATE);
        List<EventColumn> beforeColumns = Lists.newArrayList();
        EventColumn beforeColumn0 = new EventColumn("id", idVal, true, true, true);
        EventColumn beforeColumn1 = new EventColumn("name", nameValue1, false, false, true);
        beforeColumns.add(beforeColumn0);
        beforeColumns.add(beforeColumn1);
        data.setBeforeColumns(beforeColumns);

        List<EventColumn> afterColumns = Lists.newArrayList();
        EventColumn afterColumn0 = new EventColumn("id", idVal, true, true, true);
        EventColumn afterColumn1 = new EventColumn("name", nameValue2, false, false, true);
        afterColumns.add(afterColumn0);
        afterColumns.add(afterColumn1);
        data.setAfterColumns(afterColumns);

        return data;
    }

    @Test
    public void testSendAndReport() {
        boolean res = context.sendAndReport(buildUpdateEventDatas(), UPDATE, new testProducer());
        Assert.assertTrue(res);
    }
}