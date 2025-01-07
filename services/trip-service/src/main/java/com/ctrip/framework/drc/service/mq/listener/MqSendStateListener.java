package com.ctrip.framework.drc.service.mq.listener;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.service.mq.QmqProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.concurrent.CountDownLatch;

/**
 * Created by shiruixin
 * 2024/12/31 15:55
 */
public class MqSendStateListener implements MessageSendStateListener {
    private static final Logger loggerMsgSend = LoggerFactory.getLogger("MESSENGER SEND");
    protected static final String DATA_CHANGE = "dataChange";

    private CountDownLatch latch;
    private MessageProducerProvider provider;
    private QmqProducer qmqProducer;


    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void setProvider(MessageProducerProvider provider) {
        this.provider = provider;
    }

    public void setQmqProducer(QmqProducer qmqProducer) {
        this.qmqProducer = qmqProducer;
    }

    @Override
    public void onSuccess(Message message) {
        latch.countDown();
    }

    @Override
    public void onFailed(Message message) {
        loggerMsgSend.error("[[{}]]send message fail, value: {}", message.getSubject(), message.getStringProperty(DATA_CHANGE));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            loggerMsgSend.error("[Messenger] thread sleep InterruptedException: {}", e.getCause());
        }

        if (qmqProducer == null) {
            throw new RuntimeException("qmqProducer in MqSendStateListener is null");
        }

        if (qmqProducer.isUsing()) {
            retrySendMessenger(message);
        } else {
            latch.countDown();
        }
    }

    private void retrySendMessenger(Message message) {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.messenger.send.retry", message.getSubject());
        provider.sendMessage(message, this);
    }

}
