package com.ctrip.framework.drc.service.mq;

import qunar.tc.qmq.producer.MessageProducerProvider;

/**
 * Created by dengquanliang
 * 2024/5/13 15:44
 */
public class MessageProducerProviderSingleton {

    private final static  MessageProducerProvider provider;

    static {
        provider = new MessageProducerProvider();
        provider.init();
    }

    public static MessageProducerProvider getMessageProducerProvider () {
        return provider;
    }
}
