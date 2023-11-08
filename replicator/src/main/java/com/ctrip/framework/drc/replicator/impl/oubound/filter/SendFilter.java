package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.BinlogFileRegion;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class SendFilter extends AbstractPostLogEventFilter<OutboundLogEventContext> {

    private Channel channel;

    private ByteBuf bodyByteBuf;

    public SendFilter(OutboundFilterChainContext context) {
        this.channel = context.getChannel();
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        boolean skipEvent = doNext(value, value.isSkipEvent());

        bodyByteBuf = value.getBodyByteBuf();
        if (bodyByteBuf == null) {
            try {
                value.getFileChannel().position(value.getFileChannelPos() + value.getEventSize());
            } catch (IOException e) {
                logger.error("skip position error:", e);
                value.setCause(e);
                value.setSkipEvent(true);
            }
        } else {
            EventReader.releaseByteBuf(bodyByteBuf);
        }

        if (value.getCause() != null) {
            return true;
        }

        if (skipEvent) {
            return true;
        }

        if (value.isRewrite()) {
            sendRewriteEvent(value);
        } else {
            channel.writeAndFlush(new BinlogFileRegion(value.getFileChannel(), value.getFileChannelPos(), value.getEventSize()).retain());
        }

        return false;
    }

    private void sendRewriteEvent(OutboundLogEventContext value) {
        value.getLogEvent().write(byteBufs -> {
            for (ByteBuf byteBuf : byteBufs) {
                byteBuf.readerIndex(0);
                ChannelFuture future = channel.writeAndFlush(byteBuf);
                future.addListener((GenericFutureListener) f -> {
                    if (!f.isSuccess()) {
                        channel.close();
                        logger.error("[Send] {} error", channel, f.cause());
                    }
                });
            }
        });
    }

    @Override
    public void release() {
        EventReader.releaseByteBuf(bodyByteBuf);
    }
}
