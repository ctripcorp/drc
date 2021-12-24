package com.ctrip.framework.drc.replicator.impl.inbound;

import com.ctrip.framework.drc.core.driver.command.netty.codec.*;
import io.netty.channel.ChannelHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_IDLE_TIMEOUT_SECOND;

/**
 * Created by mingdongli
 * 2019/9/23 下午8:58.
 */
public class ReplicatorChannelHandlerFactory implements ChannelHandlerFactory {

    @Override
    public List<ChannelHandler> createChannelHandlers() {
        List<ChannelHandler> handlerList = new ArrayList<>();
        handlerList.add(new IdleStateHandler(CONNECTION_IDLE_TIMEOUT_SECOND, 0, 0));
        handlerList.add(new PackageEncoder());  //编码器
        handlerList.add(new UnpackDecoder());  // 拆包
        handlerList.add(new HandshakeInitializationHandler());  //握手
        handlerList.add(new AuthenticateResultHandler());  //认证
        handlerList.add(new CommandResultHandler());  //处理command
        return handlerList;
    }
}
