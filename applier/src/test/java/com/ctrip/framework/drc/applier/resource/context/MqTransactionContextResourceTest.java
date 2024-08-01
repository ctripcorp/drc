package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.activity.monitor.MqMetricsActivity;
import com.ctrip.framework.drc.applier.event.ApplierColumnsRelatedTest;
import com.ctrip.framework.drc.applier.mq.MqProvider;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.Producer;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.mq.DcTag.NON_LOCAL;
import static com.ctrip.framework.drc.core.mq.EventType.*;

/**
 * Created by jixinwang on 2022/11/16
 */
public class MqTransactionContextResourceTest implements ApplierColumnsRelatedTest {

    private static final String gtid = "6afbad2c-fabe-11e9-878b-fa163eb626bd";

    private static final String schema = "prod";
    private static final String table = "hello1";

    private MqTransactionContextResource context;

    private List<EventData> finalEventDatas;

    protected  <T extends Object> ArrayList<T> buildArray(T... items) {
        return Lists.newArrayList(items);
    }

    @Before
    public void setUp() throws Exception {
        context = new MqTransactionContextResource();
        context.updateDcTag(NON_LOCAL);
        context.setTableKey(TableKey.from(schema, table));
        context.updateGtid(gtid);
        MqProvider mockProvider = Mockito.mock(MqProvider.class);
        context.mqProvider = mockProvider;

        MqMetricsActivity mockMetricsActivity = Mockito.mock(MqMetricsActivity.class);
        context.mqMetricsActivity = mockMetricsActivity;

        Mockito.when(mockProvider.getProducers(Mockito.anyString())).thenReturn(Lists.newArrayList(new testProducer()));
    }

    @After
    public void tearDown() throws Exception {
        context.doDispose();
    }

    @Test
    public void insert() {
        context.insert(buildArray(buildArray(1, "Phi", "2019-12-09 15:00:01.000")),
                Bitmap.from(true, true, true),
                columns0());

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

    class testProducer implements Producer {

        @Override
        public String getTopic() {
            return "mockTopic";
        }

        @Override
        public void send(List<EventData> eventDatas) {
            finalEventDatas = eventDatas;
        }

        @Override
        public void destroy() {

        }
    }
}
