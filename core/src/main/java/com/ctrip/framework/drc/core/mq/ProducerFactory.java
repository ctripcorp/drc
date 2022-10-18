package com.ctrip.framework.drc.core.mq;

import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * Created by jixinwang on 2022/10/18
 */
public interface ProducerFactory extends Ordered {

    IProducer createProducer(MqConfig mqConfig);
}
