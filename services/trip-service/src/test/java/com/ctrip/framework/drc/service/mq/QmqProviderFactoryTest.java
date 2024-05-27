package com.ctrip.framework.drc.service.mq;

import org.junit.Assert;
import org.junit.Test;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by dengquanliang
 * 2024/5/27 18:51
 */
public class QmqProviderFactoryTest {

    @Test
    public void testCreateProvider() throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        Future<MessageProducerProvider> future1 = executorService.submit(() -> QmqProviderFactory.createProvider("topic"));
        Future<MessageProducerProvider> future2 = executorService.submit(() -> QmqProviderFactory.createProvider("topic"));
        Future<MessageProducerProvider> future3 = executorService.submit(() -> QmqProviderFactory.createProvider("topic"));

        MessageProducerProvider provider1 = future1.get();
        MessageProducerProvider provider2 = future2.get();
        MessageProducerProvider provider3 = future3.get();

        Assert.assertEquals(provider1, provider2);
        Assert.assertEquals(provider1, provider3);

        QmqProviderFactory.destroy("topic");
        QmqProviderFactory.destroy("topic");
        QmqProviderFactory.destroy("topic");

        MessageProducerProvider provider4 = QmqProviderFactory.createProvider("topic");
        Assert.assertNotEquals(provider1, provider4);
    }
}
