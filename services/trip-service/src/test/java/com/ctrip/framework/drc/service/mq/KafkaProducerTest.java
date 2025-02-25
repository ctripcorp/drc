package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.ckafka.client.KafkaClientFactory;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.Producer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Properties;

import static com.ctrip.framework.drc.core.mq.DcTag.NON_LOCAL;
import static com.ctrip.framework.drc.core.mq.EventType.*;

/**
 * Created by dengquanliang
 * 2025/1/20 20:29
 */
public class KafkaProducerTest {
    MockedStatic<KafkaClientFactory> theMock;
    MockedStatic<TripServiceDynamicConfig> dynamicConfigMockedStatic;
    @Before
    public void setUp() throws Exception {
        TripServiceDynamicConfig mockConfig = Mockito.mock(TripServiceDynamicConfig.class);
        Mockito.when(mockConfig.getKafkaAppidToken()).thenReturn("client.id");
        dynamicConfigMockedStatic = Mockito.mockStatic(TripServiceDynamicConfig.class);
        dynamicConfigMockedStatic.when(() -> TripServiceDynamicConfig.getInstance()).thenReturn(mockConfig);

        Producer<String, String> producer = Mockito.mock(Producer.class);
        theMock = Mockito.mockStatic(KafkaClientFactory.class);
        theMock.when(() -> KafkaClientFactory.newProducer(Mockito.anyString(), Mockito.any(Properties.class))).thenReturn(producer);
    }

    @After
    public void tearDown() throws Exception {
        theMock.close();
        dynamicConfigMockedStatic.close();
    }

    @Test
    public void testGenerateMessage() {
        MqConfig mqConfig = getMqConfig();
        KafkaProducer kafkaProducer = new KafkaProducer(mqConfig);
        Pair<String, String> messagePair = kafkaProducer.generateMessage(getDatas().get(0));
        Assert.assertEquals("key2", messagePair.getKey());

        MqConfig mqConfig1 = getMqConfig1();
        KafkaProducer kafkaProducer1 = new KafkaProducer(mqConfig1);
        Pair<String, String> messagePair1 = kafkaProducer1.generateMessage(getDatas().get(0));
        Assert.assertNull(messagePair1.getKey());
    }

    private List<EventData> getDatas() {
        EventData data = new EventData();
        data.setSchemaName("db");
        data.setTableName("table");
        data.setDcTag(NON_LOCAL);
        data.setEventType(UPDATE);

        List<EventColumn> beforeColumns = Lists.newArrayList();
        EventColumn beforeColumn0 = new EventColumn("id", "1", false, true, true);
        EventColumn beforeColumn1 = new EventColumn("name", "key1", false, false, true);
        beforeColumns.add(beforeColumn0);
        beforeColumns.add(beforeColumn1);
        data.setBeforeColumns(beforeColumns);

        List<EventColumn> afterColumns = Lists.newArrayList();
        EventColumn afterColumn0 = new EventColumn("id", "1", false, true, true);
        EventColumn afterColumn1 = new EventColumn("name", "key2", false, false, true);
        afterColumns.add(afterColumn0);
        afterColumns.add(afterColumn1);
        data.setAfterColumns(afterColumns);

        return Lists.newArrayList(data);
    }



    private MqConfig getMqConfig() {
        MqConfig mqConfig = new MqConfig();
        mqConfig.setTopic("topic");
        mqConfig.setMqType(MqType.kafka.name());
        mqConfig.setOrder(true);
        mqConfig.setOrderKey("name");
        return mqConfig;
    }

    private MqConfig getMqConfig1() {
        MqConfig mqConfig = new MqConfig();
        mqConfig.setTopic("topic");
        mqConfig.setMqType(MqType.kafka.name());
        mqConfig.setOrder(false);
        mqConfig.setOrderKey("name");
        return mqConfig;
    }

    @Test
    public void testGenerateMessageUpdate() {
        EventData data = new EventData();
        data.setSchemaName("schema");
        data.setTableName("table");
        data.setDcTag(NON_LOCAL);
        data.setEventType(UPDATE);
        List<EventColumn> beforeColumns = Lists.newArrayList();
        EventColumn beforeColumn0 = new EventColumn("id", "1", true, true, false);
        EventColumn beforeColumn1 = new EventColumn("name", null, false, false, true);
        EventColumn beforeColumn2 = new EventColumn("name2", "aaa", false, false, true);
        beforeColumns.add(beforeColumn0);
        beforeColumns.add(beforeColumn1);
        beforeColumns.add(beforeColumn2);
        data.setBeforeColumns(beforeColumns);

        List<EventColumn> afterColumns = Lists.newArrayList();
        EventColumn afterColumn0 = new EventColumn("id", "1", true, true, false);
        EventColumn afterColumn1 = new EventColumn("name", null, false, false, true);
        EventColumn afterColumn2 = new EventColumn("name2", "sss", false, false, true);
        afterColumns.add(afterColumn0);
        afterColumns.add(afterColumn1);
        afterColumns.add(afterColumn2);
        data.setAfterColumns(afterColumns);

        MqConfig config = new MqConfig();
        config.setTopic("drc.test.topic");
        config.setOrder(true);
        config.setOrderKey(null);
        KafkaProducer producer = new KafkaProducer(config);

        String messageOld = producer.generateMessageOld(data).getRight();
        String messageNew = producer.generateMessage(data).getRight();
        String cleanedStr1 = producer.removeTimestamps(messageOld);
        String cleanedStr2 = producer.removeTimestamps(messageNew);
        System.out.println(cleanedStr1);
        System.out.println(cleanedStr2);

        Assert.assertEquals(cleanedStr1,cleanedStr2);
    }



    @Test
    public void testGenerateMessageDelete() {
        EventData data = new EventData();
        data.setSchemaName("schema");
        data.setTableName("table");
        data.setDcTag(NON_LOCAL);
        data.setEventType(DELETE);
        List<EventColumn> beforeColumns = Lists.newArrayList();
        EventColumn beforeColumn0 = new EventColumn("id", "1", true, true, false);
        EventColumn beforeColumn1 = new EventColumn("name", null, false, false, true);
        EventColumn beforeColumn2 = new EventColumn("name2", "aaa", false, false, true);
        EventColumn beforeColumn3 = new EventColumn("name3", "", false, false, true);
        beforeColumns.add(beforeColumn0);
        beforeColumns.add(beforeColumn1);
        beforeColumns.add(beforeColumn2);
        beforeColumns.add(beforeColumn3);
        data.setBeforeColumns(beforeColumns);


        MqConfig config = new MqConfig();
        config.setTopic("drc.test.topic");
        config.setOrder(true);
        config.setOrderKey(null);
        KafkaProducer producer = new KafkaProducer(config);

        String messageOld = producer.generateMessageOld(data).getRight();
        String messageNew = producer.generateMessage(data).getRight();
        String cleanedStr1 = producer.removeTimestamps(messageOld);
        String cleanedStr2 = producer.removeTimestamps(messageNew);

        Assert.assertEquals(cleanedStr1,cleanedStr2);
    }

    @Test
    public void testGenerateMessageInsert() {
        EventData data = new EventData();
        data.setSchemaName("schema");
        data.setTableName("table");
        data.setDcTag(NON_LOCAL);
        data.setEventType(INSERT);

        List<EventColumn> afterColumns = Lists.newArrayList();
        EventColumn afterColumn0 = new EventColumn("id", "1", true, true, false);
        EventColumn afterColumn1 = new EventColumn("name", null, false, false, true);
        EventColumn afterColumn2 = new EventColumn("name2", "sss", false, false, true);
        EventColumn afterColumn3 = new EventColumn("name3", "", false, false, true);
        afterColumns.add(afterColumn0);
        afterColumns.add(afterColumn1);
        afterColumns.add(afterColumn2);
        afterColumns.add(afterColumn3);
        data.setAfterColumns(afterColumns);

        MqConfig config = new MqConfig();
        config.setTopic("drc.test.topic");
        config.setOrder(true);
        config.setOrderKey(null);
        KafkaProducer producer = new KafkaProducer(config);

        String messageOld = producer.generateMessageOld(data).getRight();
        String messageNew = producer.generateMessage(data).getRight();
        String cleanedStr1 = producer.removeTimestamps(messageOld);
        String cleanedStr2 = producer.removeTimestamps(messageNew);

        Assert.assertEquals(cleanedStr1,cleanedStr2);
    }
}
