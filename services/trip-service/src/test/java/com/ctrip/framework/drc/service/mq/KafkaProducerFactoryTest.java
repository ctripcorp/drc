package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.ckafka.client.KafkaClientFactory;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Properties;

/**
 * Created by dengquanliang
 * 2025/1/17 17:51
 */
public class KafkaProducerFactoryTest {

    @Before
    public void setUp() throws Exception {
        TripServiceDynamicConfig mockConfig = Mockito.mock(TripServiceDynamicConfig.class);
        Mockito.when(mockConfig.getKafkaAppidToken()).thenReturn("client.id");
        MockedStatic<TripServiceDynamicConfig> dynamicConfigMockedStatic = Mockito.mockStatic(TripServiceDynamicConfig.class);
        dynamicConfigMockedStatic.when(() -> TripServiceDynamicConfig.getInstance()).thenReturn(mockConfig);

        Producer<String, String> producer = Mockito.mock(Producer.class);
        MockedStatic<KafkaClientFactory> theMock = Mockito.mockStatic(KafkaClientFactory.class);
        theMock.when(() -> KafkaClientFactory.newProducer(Mockito.anyString(), Mockito.any(Properties.class))).thenReturn(producer);

    }

    @Test
    public void testCreateProvider() throws Exception {
        Producer<String, String> provider1 = KafkaProducerFactory.createProducer("topic");
        Assert.assertEquals(1, KafkaProducerFactory.getRefCount("topic"));

        Producer<String, String> provider2 = KafkaProducerFactory.createProducer("topic");
        Assert.assertEquals(2, KafkaProducerFactory.getRefCount("topic"));

        Assert.assertEquals(provider1, provider2);

        KafkaProducerFactory.destroy("topic");
        Assert.assertEquals(1, KafkaProducerFactory.getRefCount("topic"));
        KafkaProducerFactory.destroy("topic");


        Producer<String, String> provider3 = KafkaProducerFactory.createProducer("topic");
        Assert.assertEquals(provider1, provider3);
        Assert.assertEquals(1, KafkaProducerFactory.getRefCount("topic"));
    }
}
