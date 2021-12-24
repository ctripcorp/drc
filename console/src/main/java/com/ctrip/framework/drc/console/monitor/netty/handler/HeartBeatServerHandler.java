package com.ctrip.framework.drc.console.monitor.netty.handler;

import com.ctrip.framework.drc.core.driver.command.packet.client.HeartBeatPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-26
 */
public class HeartBeatServerHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final ByteBuf HEART_SEQUENCE = Unpooled.unreleasableBuffer(Unpooled.copiedBuffer("Heartbeat", CharsetUtil.UTF_8));

    private int currentTime = 0;

    private static final int MAX_TRY = 3;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Activated time: {}", new Date());
        logger.info("HeartBeatServerHandler channelActive");
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Inactive time：{}", new Date());
        logger.info("HeartBeatServerHandler channelInactive");
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if(evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if(event.state() == IdleState.WRITER_IDLE) {
                if(currentTime < MAX_TRY) {
                    logger.info("currentTime: {}", currentTime);
                    ctx.channel().writeAndFlush(new HeartBeatPacket());
                    currentTime++;
                }
            }
        }
    }
}
