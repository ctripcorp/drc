package com.ctrip.framework.drc.core.mq;

import com.ctrip.xpipe.api.lifecycle.Ordered;

import java.util.Set;

public interface DelayMessageConsumer extends Ordered {
    
    void initConsumer(String subject,String consumerGroup);

    boolean stopListen();

    boolean resumeListen(Set<String> mhasToBeMonitored);
}
