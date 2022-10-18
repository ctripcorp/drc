package com.ctrip.framework.drc.core.mq;

import com.ctrip.framework.drc.core.meta.MqConfig;

/**
 * Created by jixinwang on 2022/10/18
 */
public class BlankProducerFactory implements ProducerFactory {

    @Override
    public IProducer createProducer(MqConfig mqConfig) {
        return null;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
