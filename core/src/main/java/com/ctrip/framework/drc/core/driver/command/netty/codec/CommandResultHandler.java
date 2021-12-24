package com.ctrip.framework.drc.core.driver.command.netty.codec;

import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.xpipe.netty.ByteBufReadAction;
import com.ctrip.xpipe.netty.ByteBufReadPolicy;
import com.ctrip.xpipe.netty.RetryByteBufReadPolicy;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/9/8 下午5:37.
 */
public class CommandResultHandler extends ChannelDuplexHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private ByteBufReadPolicy byteBufReadPolicy = new RetryByteBufReadPolicy();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        ByteBuf byteBuf = (ByteBuf) msg;
        final NettyClient nettyClient = ctx.channel().attr(NettyClientFactory.KEY_CLIENT).get();

        byteBufReadPolicy.read(ctx.channel(), byteBuf, new ByteBufReadAction() {

            @Override
            public void read(Channel channel, ByteBuf byteBuf) {
                nettyClient.handleResponse(channel, byteBuf);
            }
        });

        super.channelRead(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("[Caught] exception", cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            switch (e.state()) {
                case READER_IDLE:
                    if (!ctx.channel().config().isAutoRead()) {
                        logger.info("[READER_IDLE] fire, but auto read is false, return");
                        ctx.fireUserEventTriggered(evt);
                        return;
                    }
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.mysql.readidle", ctx.channel().remoteAddress().toString());
                    ctx.close();
                    logger.warn("[READER_IDLE] fire and close channel");
                    break;
                default:
                    break;
            }
        } else {
            logger.info("receive {} event for {}", evt.toString(), ctx.channel().toString());
        }
        ctx.fireUserEventTriggered(evt);
    }
}
