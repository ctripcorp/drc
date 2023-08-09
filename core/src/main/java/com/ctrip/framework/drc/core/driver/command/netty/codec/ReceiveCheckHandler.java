package com.ctrip.framework.drc.core.driver.command.netty.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_IDLE_TIMEOUT_SECOND;

/**
 * Created by jixinwang on 2023/8/8
 */
public class ReceiveCheckHandler extends ChannelInboundHandlerAdapter {

    private static final int CHECK_PERIOD = CONNECTION_IDLE_TIMEOUT_SECOND * 2;

    private boolean msgReceived = true;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        msgReceived = true;
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.executor().scheduleWithFixedDelay(() -> {
            if (!msgReceived) {
                msgReceived = true;
                ctx.close();
            }
            msgReceived = false;
        }, CHECK_PERIOD, CHECK_PERIOD, TimeUnit.SECONDS);
    }
}
