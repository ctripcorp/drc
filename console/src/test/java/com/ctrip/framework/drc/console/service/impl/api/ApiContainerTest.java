package com.ctrip.framework.drc.console.service.impl.api;

import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.framework.drc.core.mq.IKafkaDelayMessageConsumer;
import org.junit.Test;

/**
 * Created by shiruixin
 * 2025/1/10 15:37
 */
public class ApiContainerTest {
    @Test
    public void name() {
        DelayMessageConsumer delayMessageConsumer = ApiContainer.getDelayMessageConsumer();
        IKafkaDelayMessageConsumer delayMessageConsumer1 = ApiContainer.getKafkaDelayMessageConsumer();
    }
}