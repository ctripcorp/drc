package com.ctrip.framework.drc.service.mq.listener;

import com.ctrip.framework.drc.service.mq.QmqProducer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.concurrent.CountDownLatch;

/**
 * Created by shiruixin
 * 2025/1/2 11:12
 */
public class MqSendStateListenerTest {

    private MqSendStateListener listener;
    private MessageProducerProvider provider;
    private QmqProducer qmqProducer;

    private Message message;

    private CountDownLatch latch;

    @Before
    public void setUp() throws Exception {
        provider = Mockito.mock(MessageProducerProvider.class);
        message = Mockito.mock(Message.class);
        qmqProducer = Mockito.mock(QmqProducer.class);
        listener = new MqSendStateListener();
        listener.setProvider(provider);
        latch = new CountDownLatch(2);
        listener.setLatch(latch);
        listener.setQmqProducer(qmqProducer);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSendSuccess() {
        listener.onSuccess(message);
        Assert.assertEquals(1, latch.getCount());
    }

    @Test
    public void testSendFail() {
        Mockito.when(qmqProducer.isUsing()).thenReturn(true);
        listener.onFailed(message);
        Mockito.verify(provider,Mockito.times(1)).sendMessage(message, listener);
        Assert.assertEquals(2, latch.getCount());
    }

    @Test
    public void testSendFailButProducerStopping() {
        Mockito.when(qmqProducer.isUsing()).thenReturn(false);
        listener.onFailed(message);
        Mockito.verifyNoInteractions(provider);
        Assert.assertEquals(1, latch.getCount());
    }

}