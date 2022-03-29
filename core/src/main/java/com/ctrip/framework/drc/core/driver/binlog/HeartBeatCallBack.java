package com.ctrip.framework.drc.core.driver.binlog;

import com.ctrip.framework.drc.core.driver.command.packet.client.HeartBeatResponsePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.HEARTBEAT_LOGGER;

/**
 * @Author limingdong
 * @create 2022/3/29
 */
public interface HeartBeatCallBack {

    Channel getChannel();

    default void onHeartHeat() {
        Channel channel = getChannel();
        try {
            HeartBeatResponsePacket heartBeatResponsePacket = new HeartBeatResponsePacket();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
            heartBeatResponsePacket.write(byteBuf);
            ChannelFuture future = channel.writeAndFlush(byteBuf);
            future.addListener((GenericFutureListener) f -> {
                if (!f.isSuccess()) {
                    channel.close();
                    HEARTBEAT_LOGGER.error("[Remove] {} due to sending HeartBeatResponsePacket error", channel);
                } else {
                    HEARTBEAT_LOGGER.info("[Send] {} HeartBeatResponsePacket", channel);
                }
            });
        } catch (Exception e) {
            HEARTBEAT_LOGGER.error("[Send] {} HeartBeatResponsePacket error", channel);
        }
    }
}
