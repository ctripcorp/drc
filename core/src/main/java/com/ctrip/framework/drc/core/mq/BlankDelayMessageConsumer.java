package com.ctrip.framework.drc.core.mq;

/**
 * @ClassName BlankDelayMessageConsumer
 * @Author haodongPan
 * @Date 2022/11/10 16:08
 * @Version: $
 */
public class BlankDelayMessageConsumer implements DelayMessageConsumer {
    @Override
    public void initConsumer() {}

    @Override
    public boolean stopListen() {
        return false;
    }

    @Override
    public boolean resumeListen() {
        return false;
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
