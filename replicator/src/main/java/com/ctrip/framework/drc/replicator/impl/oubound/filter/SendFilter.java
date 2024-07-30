package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.BinlogFileRegion;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.sender.SenderFilterChainContext;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class SendFilter extends AbstractPostLogEventFilter<OutboundLogEventContext> {

    private Channel channel;

    public SendFilter(SenderFilterChainContext context) {
        this.channel = context.getChannel();
    }


    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        boolean skipEvent = doNext(value, value.isSkipEvent());

        if (value.getCause() != null) {
            // currently only extractFilter may throw this error
            return true;
        }

        if (skipEvent) {
            return true;
        }

        this.doSend(value);

        return false;
    }

    protected void doSend(OutboundLogEventContext value) {
        if (value.isRewrite()) {
            sendRewriteEvent(value);
        } else {
            channel.writeAndFlush(new BinlogFileRegion(value.getFileChannel(), value.getFileChannelPos(), value.getEventSize()).retain());
        }
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
}
