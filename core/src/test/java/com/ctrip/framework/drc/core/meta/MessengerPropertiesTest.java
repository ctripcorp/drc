package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.mq.DefaultProducerFactoryHolder;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.mq.Producer;
import com.ctrip.framework.drc.core.mq.ProducerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public class MessengerPropertiesTest {

    MockedStatic<DefaultProducerFactoryHolder> mockedStatic;

    private String config = "{\n" +
            "        \"mqConfigs\": [\n" +
            "            {\n" +
            "                \"mqType\": \"qmq\",\n" +
            "                \"table\": \"d1\",\n" +
            "                \"topic\": \"topicName\",\n" +
            "                \"serialization\": \"arvo\",\n" +
            "                \"persistent\": true,\n" +
            "                \"persistentDb\": \"db1\",\n" +
            "                \"order\": true,\n" +
            "                \"orderKey\": \"key1\",\n" +
            "                \"delayTime\": 123,\n" +
            "                \"processor\": \"java\"\n" +
            "            }\n" +
            "        ]\n" +
            "    }";

    @Test
    public void from() {

        MessengerProperties messengerProperties = null;
        try {
            messengerProperties = MessengerProperties.from(config);
            Assert.assertTrue(messengerProperties.getMqConfigs().get(0).isOrder());
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }
    @Before
    public void setUp() {
        Producer mockProducer = Mockito.mock(Producer.class);
        ProducerFactory mockFactory = Mockito.mock(ProducerFactory.class);
        Mockito.when(mockFactory.createProducer(Mockito.any())).thenReturn(mockProducer);
        mockedStatic = Mockito.mockStatic(DefaultProducerFactoryHolder.class);
        mockedStatic.when(() -> DefaultProducerFactoryHolder.getInstance()).thenReturn(mockFactory);
    }

    @After
    public void tearDown() {
        mockedStatic.close();
    }

    @Test
    public void testGetProducers() throws Exception{
        String configSameRegexMultiTopic = "{\n" +
                "        \"mqConfigs\": [\n" +
                "            {\n" +
                "                \"mqType\": \"qmq\",\n" +
                "                \"table\": \"tbl.*\",\n" +
                "                \"topic\": \"topicName1\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"mqType\": \"qmq\",\n" +
                "                \"table\": \"tbl.*\",\n" +
                "                \"topic\": \"topicName2\"\n" +
                "            }\n" +
                "        ]\n" +
                "    }";

        MessengerProperties messengerProperties = MessengerProperties.from(configSameRegexMultiTopic);
        List<Producer> producers = messengerProperties.getProducers("tbl1");
        Assert.assertEquals(producers.size(),2);
        producers = messengerProperties.getProducers("tbl1");
        Assert.assertEquals(producers.size(),2);


        producers = messengerProperties.getProducers("t1");
        Assert.assertEquals(producers.size(),0);

        String configDifferentRegexSameTableMultiTopic = "{\n" +
                "        \"mqConfigs\": [\n" +
                "            {\n" +
                "                \"mqType\": \"qmq\",\n" +
                "                \"table\": \"tbl.*\",\n" +
                "                \"topic\": \"topicName1\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"mqType\": \"qmq\",\n" +
                "                \"table\": \"tbl_.*\",\n" +
                "                \"topic\": \"topicName2\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"mqType\": \"qmq\",\n" +
                "                \"table\": \"tbl1\",\n" +
                "                \"topic\": \"topicName3\"\n" +
                "            }\n" +
                "        ]\n" +
                "    }";
        messengerProperties = MessengerProperties.from(configDifferentRegexSameTableMultiTopic);
        producers = messengerProperties.getProducers("tbl1");
        Assert.assertEquals(producers.size(),2);
        producers = messengerProperties.getProducers("tbl1");
        Assert.assertEquals(producers.size(),2);
        producers = messengerProperties.getProducers("tbl_1");
        Assert.assertEquals(producers.size(),2);
        producers = messengerProperties.getProducers("tbl_1");
        Assert.assertEquals(producers.size(),2);
        producers = messengerProperties.getProducers("tbl22");
        Assert.assertEquals(producers.size(),1);
    }
}
