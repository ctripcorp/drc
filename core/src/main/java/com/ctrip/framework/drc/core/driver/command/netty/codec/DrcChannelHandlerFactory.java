package com.ctrip.framework.drc.core.driver.command.netty.codec;

import io.netty.channel.ChannelHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_IDLE_TIMEOUT_SECOND;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class DrcChannelHandlerFactory implements ChannelHandlerFactory {

    @Override
    public List<ChannelHandler> createChannelHandlers() {
        List<ChannelHandler> handlerList = new ArrayList<>();
        handlerList.add(new IdleStateHandler(CONNECTION_IDLE_TIMEOUT_SECOND, 0, 0));
        handlerList.add(new PackageEncoder());
        handlerList.add(new FileEventDecode());
        handlerList.add(new CommandResultHandler());
        return handlerList;
    }
}
