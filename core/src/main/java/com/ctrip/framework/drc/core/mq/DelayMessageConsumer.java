package com.ctrip.framework.drc.core.mq;

import com.ctrip.xpipe.api.lifecycle.Ordered;

public interface DelayMessageConsumer extends Ordered {
    
    void initConsumer();

    boolean stopListen();

    boolean resumeListen();
}
