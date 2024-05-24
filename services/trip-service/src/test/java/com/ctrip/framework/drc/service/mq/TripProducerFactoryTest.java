package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.mq.Producer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dengquanliang
 * 2024/5/24 17:30
 */
public class TripProducerFactoryTest {

    @Test
    public void testCreateProduce() {
        TripProducerFactory tripProducerFactory = new TripProducerFactory();
        MqConfig mqConfig1 = getMqConfig("topic");
        MqConfig mqConfig2 = getMqConfig("topic");
        MqConfig mqConfig3 = getMqConfig("topic");

        Producer producer1 = tripProducerFactory.createProducer(mqConfig1);
        Producer producer2 = tripProducerFactory.createProducer(mqConfig2);
        Assert.assertEquals(producer1, producer2);
        Assert.assertEquals(producer1.getRefCount(), 2);

        producer1.destroy();
        Assert.assertEquals(producer1.getRefCount(), 1);
        producer1.destroy();
        Assert.assertEquals(producer1.getRefCount(), 0);

        Producer producer3 = tripProducerFactory.createProducer(mqConfig3);
        Assert.assertNotEquals(producer1, producer3);
    }

    private MqConfig getMqConfig(String topic) {
        MqConfig mqConfig = new MqConfig();
        mqConfig.setMqType(MqType.qmq.name());
        mqConfig.setTopic(topic);
        return mqConfig;
    }


}