package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import io.netty.channel.Channel;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundFilterChainContext {

    private Channel channel;

    private ConsumeType consumeType;

    private DataMediaConfig dataMediaConfig;

    public OutboundFilterChainContext(Channel channel, ConsumeType consumeType, DataMediaConfig dataMediaConfig) {
        this.channel = channel;
        this.consumeType = consumeType;
        this.dataMediaConfig = dataMediaConfig;
    }

    public Channel getChannel() {
        return channel;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public boolean shouldFilterRows() {
        if (dataMediaConfig == null) {
            return false;
        }
        return dataMediaConfig.shouldFilterRows();
    }

    public DataMediaConfig getDataMediaConfig() {
        return dataMediaConfig;
    }

    public static OutboundFilterChainContext from(Channel channel, ConsumeType consumeType, DataMediaConfig dataMediaConfig) {
        return new OutboundFilterChainContext(channel, consumeType, dataMediaConfig);
    }

}
