package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import qunar.tc.qmq.Message;

import java.util.List;

import static com.ctrip.framework.drc.core.mq.DcTag.NON_LOCAL;
import static com.ctrip.framework.drc.core.mq.EventType.UPDATE;
import static com.ctrip.framework.drc.service.mq.QmqProducer.DATA_CHANGE;

/**
 * Created by jixinwang on 2022/11/17
 */
public class QmqProducerTest {

    private String schema = "testdb";
    private String table = "table";

    @Test
    public void generateMessage()  {
        TripServiceDynamicConfig mockConfig = Mockito.mock(TripServiceDynamicConfig.class);
        Mockito.when(mockConfig.isSubenvEnable()).thenReturn(false);
        MockedStatic<TripServiceDynamicConfig> theMock = Mockito.mockStatic(TripServiceDynamicConfig.class);
        theMock.when(() -> TripServiceDynamicConfig.getInstance()).thenReturn(mockConfig);

        MqConfig config = new MqConfig();
        config.setTopic("drc.test.topic");
        config.setOrder(true);
        config.setOrderKey("id");
        QmqProducer producer = new QmqProducer(config);

        EventData data = new EventData();
        data.setSchemaName(schema);
        data.setTableName(table);
        data.setDcTag(NON_LOCAL);
        data.setEventType(UPDATE);

        List<EventColumn> beforeColumns = Lists.newArrayList();
        EventColumn beforeColumn0 = new EventColumn("id", "1", false, true, true);
        EventColumn beforeColumn1 = new EventColumn("name", "Mike", false, false, true);
        beforeColumns.add(beforeColumn0);
        beforeColumns.add(beforeColumn1);
        data.setBeforeColumns(beforeColumns);

        List<EventColumn> afterColumns = Lists.newArrayList();
        EventColumn afterColumn0 = new EventColumn("id", "1", false, true, true);
        EventColumn afterColumn1 = new EventColumn("name", "Joe", false, false, true);
        afterColumns.add(afterColumn0);
        afterColumns.add(afterColumn1);
        data.setAfterColumns(afterColumns);

        Message message = producer.generateMessage(data);
        String dataChange = message.getStringProperty(DATA_CHANGE);
        JSONObject jsonObject = JSON.parseObject(dataChange);

        Assert.assertEquals(UPDATE.name(), jsonObject.getString("eventType"));
        Assert.assertEquals(schema, jsonObject.getString("schemaName"));
        Assert.assertEquals(table, jsonObject.getString("tableName"));
        Assert.assertEquals(NON_LOCAL.getName(), jsonObject.getString("dc"));

        JSONObject orderKeyInfo = jsonObject.getJSONObject("orderKeyInfo");
        Assert.assertEquals(schema, orderKeyInfo.getString("schemaName"));
        Assert.assertEquals(table, orderKeyInfo.getString("tableName"));
        JSONArray pks = orderKeyInfo.getJSONArray("pks");
        Assert.assertEquals("1", pks.getString(0));

        JSONArray beforeColumnList = jsonObject.getJSONArray("beforeColumnList");
        JSONObject beforeId = beforeColumnList.getJSONObject(0);
        Assert.assertEquals("id", beforeId.getString("name"));
        Assert.assertEquals("1", beforeId.getString("value"));
        Assert.assertEquals(false, beforeId.getBoolean("isNull"));
        Assert.assertEquals(true, beforeId.getBoolean("isKey"));
        Assert.assertEquals(true, beforeId.getBoolean("isUpdated"));
        JSONObject beforeName = beforeColumnList.getJSONObject(1);
        Assert.assertEquals("name", beforeName.getString("name"));
        Assert.assertEquals("Mike", beforeName.getString("value"));
        Assert.assertEquals(false, beforeName.getBoolean("isNull"));
        Assert.assertEquals(false, beforeName.getBoolean("isKey"));
        Assert.assertEquals(true, beforeName.getBoolean("isUpdated"));

        JSONArray afterColumnList = jsonObject.getJSONArray("afterColumnList");
        JSONObject afterId = afterColumnList.getJSONObject(0);
        Assert.assertEquals("id", afterId.getString("name"));
        Assert.assertEquals("1", afterId.getString("value"));
        Assert.assertEquals(false, afterId.getBoolean("isNull"));
        Assert.assertEquals(true, afterId.getBoolean("isKey"));
        Assert.assertEquals(true, afterId.getBoolean("isUpdated"));
        JSONObject afterName = afterColumnList.getJSONObject(1);
        Assert.assertEquals("name", afterName.getString("name"));
        Assert.assertEquals("Joe", afterName.getString("value"));
        Assert.assertEquals(false, afterName.getBoolean("isNull"));
        Assert.assertEquals(false, afterName.getBoolean("isKey"));
        Assert.assertEquals(true, afterName.getBoolean("isUpdated"));

        theMock.close();
    }

    @Test
    public void testGenerateMessageSendByPks()  {
        TripServiceDynamicConfig mockConfig = Mockito.mock(TripServiceDynamicConfig.class);
        Mockito.when(mockConfig.isSubenvEnable()).thenReturn(false);
        MockedStatic<TripServiceDynamicConfig> theMock = Mockito.mockStatic(TripServiceDynamicConfig.class);
        theMock.when(() -> TripServiceDynamicConfig.getInstance()).thenReturn(mockConfig);

        MqConfig config = new MqConfig();
        config.setTopic("drc.test.topic");
        config.setOrder(true);
        config.setOrderKey(null);
        QmqProducer producer = new QmqProducer(config);

        EventData data = new EventData();
        data.setSchemaName(schema);
        data.setTableName(table);
        data.setDcTag(NON_LOCAL);
        data.setEventType(UPDATE);
        List<EventColumn> beforeColumns = Lists.newArrayList();
        EventColumn beforeColumn0 = new EventColumn("id", "1", false, true, true);
        EventColumn beforeColumn1 = new EventColumn("name", "Mike", false, false, true);
        beforeColumns.add(beforeColumn0);
        beforeColumns.add(beforeColumn1);
        data.setBeforeColumns(beforeColumns);

        List<EventColumn> afterColumns = Lists.newArrayList();
        EventColumn afterColumn0 = new EventColumn("id", "1", false, true, true);
        EventColumn afterColumn1 = new EventColumn("name", "Joe", false, false, true);
        afterColumns.add(afterColumn0);
        afterColumns.add(afterColumn1);
        data.setAfterColumns(afterColumns);

        Message message = producer.generateMessage(data);
        Assert.assertEquals("testdb.table_1", message.getOrderKey());

        beforeColumns = Lists.newArrayList();
        beforeColumn0 = new EventColumn("id", "1", false, true, true);
        beforeColumn1 = new EventColumn("name", "Mike", false, true, true);
        beforeColumns.add(beforeColumn0);
        beforeColumns.add(beforeColumn1);
        data.setBeforeColumns(beforeColumns);

        afterColumns = Lists.newArrayList();
        afterColumn0 = new EventColumn("id", "1", false, true, true);
        afterColumn1 = new EventColumn("name", "Joe", false, true, true);
        afterColumns.add(afterColumn0);
        afterColumns.add(afterColumn1);
        data.setAfterColumns(afterColumns);

        message = producer.generateMessage(data);
        Assert.assertEquals("testdb.table_1_Joe", message.getOrderKey());

        beforeColumns = Lists.newArrayList();
        beforeColumn0 = new EventColumn("id", "1", false, false, true);
        beforeColumn1 = new EventColumn("name", "Mike", false, false, true);
        beforeColumns.add(beforeColumn0);
        beforeColumns.add(beforeColumn1);
        data.setBeforeColumns(beforeColumns);

        afterColumns = Lists.newArrayList();
        afterColumn0 = new EventColumn("id", "1", false, false, true);
        afterColumn1 = new EventColumn("name", "Joe", false, false, true);
        afterColumns.add(afterColumn0);
        afterColumns.add(afterColumn1);
        data.setAfterColumns(afterColumns);

        message = producer.generateMessage(data);
        Assert.assertEquals("testdb.table", message.getOrderKey());

        theMock.close();
    }
}
