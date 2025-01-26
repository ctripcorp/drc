package com.ctrip.framework.drc.core.mq;

import com.ctrip.framework.drc.core.meta.MqConfig;
import com.google.common.collect.Lists;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.MESSENGER_DELAY_MONITOR_TOPIC;

/**
 * Created by jixinwang on 2022/11/11
 */
public class DelayMessageProducer {

    private static class ProducerHolder {
        public static final List<Producer> QMQ_INSTANCE = createQmqMonitorProducer();
        public static final List<Producer> KAFKA_INSTANCE = createKafkaMonitorProducer();
    }

    public static List<Producer> getInstance(String mqType) {
        if (mqType.equalsIgnoreCase(MqType.qmq.name())) {
            return getQmqInstance();
        } else if (mqType.equalsIgnoreCase(MqType.kafka.name())) {
            return getKafkaInstance();
        } else {
            throw new IllegalStateException("unsupported mqType");
        }
    }

    public static List<Producer> getQmqInstance() {
        return DelayMessageProducer.ProducerHolder.QMQ_INSTANCE;
    }

    public static List<Producer> getKafkaInstance() {
        return DelayMessageProducer.ProducerHolder.KAFKA_INSTANCE;
    }

    private static List<Producer> createQmqMonitorProducer() {
        MqConfig monitorMqConfig = new MqConfig();
        monitorMqConfig.setMqType(MqType.qmq.name());
        monitorMqConfig.setTopic(MESSENGER_DELAY_MONITOR_TOPIC);
        monitorMqConfig.setOrder(true);
        monitorMqConfig.setOrderKey("id");
        Producer producer = DefaultProducerFactoryHolder.getInstance().createProducer(monitorMqConfig);
        return Lists.newArrayList(producer);
    }

    private static List<Producer> createKafkaMonitorProducer() {
        MqConfig monitorMqConfig = new MqConfig();
        monitorMqConfig.setMqType(MqType.kafka.name());
        monitorMqConfig.setTopic(MESSENGER_DELAY_MONITOR_TOPIC);
        monitorMqConfig.setOrder(true);
        monitorMqConfig.setOrderKey("id");
        Producer producer = DefaultProducerFactoryHolder.getInstance().createProducer(monitorMqConfig);
        return Lists.newArrayList(producer);
    }
}
