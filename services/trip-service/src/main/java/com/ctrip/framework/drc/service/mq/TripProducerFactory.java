package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.mq.IProducer;
import com.ctrip.framework.drc.core.mq.ProducerFactory;

/**
 * Created by jixinwang on 2022/10/17
 */
public class TripProducerFactory implements ProducerFactory {

    @Override
    public IProducer createProducer(MqConfig mqConfig) {
        if ("qmq".equalsIgnoreCase(mqConfig.getMqType())) {
            return new QmqProducer(mqConfig);
        }
        return null;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
