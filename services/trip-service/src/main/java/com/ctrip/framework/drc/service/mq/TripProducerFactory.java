package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.mq.Producer;
import com.ctrip.framework.drc.core.mq.ProducerFactory;

/**
 * Created by jixinwang on 2022/10/17
 */
public class TripProducerFactory implements ProducerFactory {

    @Override
    public Producer createProducer(MqConfig mqConfig) {
        if (MqType.qmq.name().equalsIgnoreCase(mqConfig.getMqType())) {
            return new QmqProducer(mqConfig);
        } else if (MqType.kafka.name().equalsIgnoreCase(mqConfig.getMqType())) {
            return new KafkaProducer(mqConfig);
        }
        throw new UnsupportedOperationException("unSupport mq type: " + mqConfig.getMqType());
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
