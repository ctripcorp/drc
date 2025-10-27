package com.ctrip.framework.drc.fetcher.activity.replicator.handler.command;

import com.ctrip.framework.drc.core.driver.command.netty.codec.CommandResultHandler;
import com.ctrip.framework.drc.core.driver.command.packet.client.HeartBeatResponsePacket;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.GenericFutureListener;

import static com.ctrip.framework.drc.core.driver.binlog.HeartBeatCallBack.AUTO_READ_CLOSE;
import static com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory.KEY_LAST_PROCESS_TIME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.APPLIER_PROCESS_EVENT_MAX_IDLE_TIMEOUT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.HEARTBEAT_LOGGER;

/**
 * Created by shiruixin
 * 2025/10/11 14:40
 */
public class FetcherCommandResultHandler extends CommandResultHandler {

    /**
     * Add heartbeat response handling during WRITER_IDLE compared to the parent class,
     * to address scenarios with backlog in applier and messenger processing.
     * @see com.ctrip.framework.drc.core.driver.command.netty.codec.CommandResultHandler#userEventTriggered(ChannelHandlerContext, Object)
     */
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
                case WRITER_IDLE:
                    Long applierLastProcessTime = ctx.channel().attr(KEY_LAST_PROCESS_TIME).get();
                    if (!ctx.channel().config().isAutoRead()
                            || (applierLastProcessTime != null && System.currentTimeMillis() - applierLastProcessTime < APPLIER_PROCESS_EVENT_MAX_IDLE_TIMEOUT)) {
                        handleWriterIdle(ctx);
                        DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.writeidle.heartbeat", ctx.channel().remoteAddress().toString());
                    } else {
                        String reason = ctx.channel().config().isAutoRead() ? (applierLastProcessTime == null ? "applierLastProcessTime is null" : "timeSinceLastEvent exceed limit") : "auto read is false";
                        logger.warn("[WRITE_IDLE] skip sending heartbeat response for {}, reason: {}", ctx.channel(), reason);
                        DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.writeidle.heartbeat.skip", ctx.channel().remoteAddress().toString());
                    }

                default:
                    break;
            }
        } else {
            logger.info("receive {} event for {}", evt.toString(), ctx.channel().toString());
        }
        ctx.fireUserEventTriggered(evt);
    }

    private void handleWriterIdle(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        try {
            HeartBeatResponsePacket heartBeatResponsePacket = new HeartBeatResponsePacket(AUTO_READ_CLOSE);
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
            heartBeatResponsePacket.write(byteBuf);
            ChannelFuture future = channel.writeAndFlush(byteBuf);
            long startTime = System.currentTimeMillis();
            future.addListener((GenericFutureListener) f -> {
                long endTime = System.currentTimeMillis();
                if (!f.isSuccess()) {
                    ctx.close();
                    HEARTBEAT_LOGGER.error("[WRITER_IDLE][Remove] {} due to sending HeartBeatResponsePacket error, time cost: {}", channel, endTime - startTime);
                } else {
                    HEARTBEAT_LOGGER.info("[WRITER_IDLE][Send] {} HeartBeatResponsePacket, time cost: {}", channel, endTime - startTime);
                }
            });
        } catch (Exception e) {
            ctx.close();
            HEARTBEAT_LOGGER.error("[WRITER_IDLE][Remove] {} HeartBeatResponsePacket error. close channel", channel, e);
        }
    }
}
