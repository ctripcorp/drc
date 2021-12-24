package com.ctrip.framework.drc.core.driver.command.netty.codec;

import io.netty.channel.ChannelHandler;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/9/23 下午8:56.
 */
public interface ChannelHandlerFactory {

    List<ChannelHandler> createChannelHandlers();

}
