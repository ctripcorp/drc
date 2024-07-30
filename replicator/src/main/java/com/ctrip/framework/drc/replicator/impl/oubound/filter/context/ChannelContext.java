package com.ctrip.framework.drc.replicator.impl.oubound.filter.context;

import io.netty.channel.Channel;

/**
 * @author yongnian
 */
public interface ChannelContext  {
    Channel getChannel();
}
