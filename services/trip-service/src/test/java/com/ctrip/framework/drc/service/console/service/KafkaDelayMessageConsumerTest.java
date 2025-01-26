package com.ctrip.framework.drc.service.console.service;

import com.ctrip.framework.ckafka.client.KafkaClientFactory;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Properties;
/**
  * Created by shiruixin
  * 2025/1/10 16:57
  */
public class KafkaDelayMessageConsumerTest {

    private MockedStatic<TripServiceDynamicConfig> dynamicConfigMockedStatic;
    private MockedStatic<DefaultReporterHolder> defaultReporterHolderMockedStatic;
    private MockedStatic<KafkaClientFactory> theMock;

    @Before
    public void setUp() throws Exception {
        TripServiceDynamicConfig mockConfig = Mockito.mock(TripServiceDynamicConfig.class);
        Mockito.when(mockConfig.getKafkaAppidToken()).thenReturn("client.id");
        dynamicConfigMockedStatic = Mockito.mockStatic(TripServiceDynamicConfig.class);
        dynamicConfigMockedStatic.when(() -> TripServiceDynamicConfig.getInstance()).thenReturn(mockConfig);

        Reporter reporter = Mockito.mock(Reporter.class);
        defaultReporterHolderMockedStatic = Mockito.mockStatic(DefaultReporterHolder.class);
        defaultReporterHolderMockedStatic.when(() -> DefaultReporterHolder.getInstance()).thenReturn(reporter);

        Consumer<String,String> consumer = Mockito.mock(Consumer.class);
        theMock = Mockito.mockStatic(KafkaClientFactory.class);
        theMock.when(() -> KafkaClientFactory.newConsumer(Mockito.anyString(), Mockito.anyString(), Mockito.any(Properties.class))).thenReturn(consumer);

    }

    @After
    public void tearDown() throws Exception {
        dynamicConfigMockedStatic.close();
        defaultReporterHolderMockedStatic.close();
        theMock.close();
    }

    @Test
    public void testConsume() {
        KafkaDelayMessageConsumer kafkaDelayMessageConsumer = new KafkaDelayMessageConsumer();
        kafkaDelayMessageConsumer.initConsumer("subject", "consumerGroup", null);
        boolean res = kafkaDelayMessageConsumer.resumeConsume();
        Assert.assertTrue(res);

        res = kafkaDelayMessageConsumer.resumeConsume();
        Assert.assertFalse(res);

        res = kafkaDelayMessageConsumer.stopConsume();
        Assert.assertTrue(res);
        res = kafkaDelayMessageConsumer.stopConsume();
        Assert.assertTrue(res);

        res = kafkaDelayMessageConsumer.resumeConsume();
        Assert.assertTrue(res);

        kafkaDelayMessageConsumer.stopConsume();
    }

}