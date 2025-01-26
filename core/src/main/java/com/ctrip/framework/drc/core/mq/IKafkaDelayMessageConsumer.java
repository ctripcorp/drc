package com.ctrip.framework.drc.core.mq;

import com.ctrip.xpipe.api.lifecycle.Ordered;

import java.util.Map;
import java.util.Set;

/**
 * Created by shiruixin
 * 2025/1/10 16:06
 */
public interface IKafkaDelayMessageConsumer extends Ordered {
    void initConsumer(String subject, String consumerGroup, Set<String> dcs);

    void mhasRefresh(Set<String> mhas, Map<String, String> mha2Dc);

    boolean stopConsume();

    boolean resumeConsume();
}
