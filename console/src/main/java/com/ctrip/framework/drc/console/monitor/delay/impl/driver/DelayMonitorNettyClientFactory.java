package com.ctrip.framework.drc.console.monitor.delay.impl.driver;

import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-12-24
 */
public class DelayMonitorNettyClientFactory extends NettyClientFactory {

    public DelayMonitorNettyClientFactory(Endpoint endpoint, String threadPrefix, boolean autoRead) {
        super(endpoint, threadPrefix, autoRead);
    }

    @Override
    protected RecvByteBufAllocator getRecvByteBufAllocator() {
        return new AdaptiveRecvByteBufAllocator();
    }
}
