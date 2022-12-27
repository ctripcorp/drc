package com.ctrip.framework.drc.core.mq;

import com.ctrip.xpipe.api.lifecycle.Ordered;

import java.util.Set;

public interface DelayMessageConsumer extends Ordered {
    
    void initConsumer(String subject,String consumerGroup,Set<String> dcs);

    void mhasRefresh(Set<String> mhas);

    boolean stopListen();

    boolean resumeListen();
}
