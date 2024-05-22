package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.mq.Producer;
import com.ctrip.framework.drc.core.mq.ProducerFactory;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by jixinwang on 2022/10/17
 */
public class TripProducerFactory implements ProducerFactory {

    private Map<String, Producer> topicToProducer = Maps.newConcurrentMap();

    @Override
    public Producer createProducer(MqConfig mqConfig) {
        if (MqType.qmq.name().equalsIgnoreCase(mqConfig.getMqType())) {
            Producer producer = topicToProducer.get(mqConfig.getTopic());
            if (producer == null || producer.getRefCount() == 0) {
                synchronized (this) {
                    producer = topicToProducer.get(mqConfig.getTopic());
                    if (producer == null || producer.getRefCount() == 0) {
                        producer = new QmqProducer(mqConfig);
                        topicToProducer.put(mqConfig.getTopic(), producer);
                        return producer;
                    }
                }
            }
            producer.increaseRefCount();
            return producer;
        }
        throw new UnsupportedOperationException("unSupport mq type: " + mqConfig.getMqType());
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
