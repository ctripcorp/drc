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
        public static final List<Producer> INSTANCE = createMonitorProducer();
    }

    public static List<Producer> getInstance() {
        return DelayMessageProducer.ProducerHolder.INSTANCE;
    }

    private static List<Producer> createMonitorProducer() {
        MqConfig monitorMqConfig = new MqConfig();
        monitorMqConfig.setMqType(MqType.qmq.name());
        monitorMqConfig.setTopic(MESSENGER_DELAY_MONITOR_TOPIC);
        monitorMqConfig.setOrder(true);
        monitorMqConfig.setOrderKey("id");
        Producer producer = DefaultProducerFactoryHolder.getInstance().createProducer(monitorMqConfig);
        return Lists.newArrayList(producer);
    }
}
