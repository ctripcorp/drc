package com.ctrip.framework.drc.core.mq;

import java.util.Map;
import java.util.Set;

/**
 * @ClassName BlankDelayMessageConsumer
 * @Author haodongPan
 * @Date 2022/11/10 16:08
 * @Version: $
 */
public class BlankDelayMessageConsumer implements DelayMessageConsumer {
    
    @Override
    public void initConsumer(String subject, String consumerGroup, Set<String> dcs) {}

    @Override
    public boolean stopListen() {
        return false;
    }

    @Override
    public boolean resumeListen() {
        return false;
    }

    @Override
    public void mhasRefresh(Map<String, String> mhas2Dc) {

    }

    @Override
    public int getOrder() {
        return 1;
    }
}
