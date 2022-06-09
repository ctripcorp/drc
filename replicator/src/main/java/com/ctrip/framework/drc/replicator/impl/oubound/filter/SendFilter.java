package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.BinlogFileRegion;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.Collection;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class SendFilter extends AbstractPostLogEventFilter<OutboundLogEventContext> {

    private Channel channel;

    public SendFilter(Channel channel) {
        this.channel = channel;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        boolean noRowFiltered = doNext(value, value.isNoRowFiltered());
        if (value.getCause() != null) {
            return noRowFiltered;
        }

        if (noRowFiltered) {
            channel.writeAndFlush(new BinlogFileRegion(value.getFileChannel(), value.getFileChannelPos() - eventHeaderLengthVersionGt1, value.getEventSize()).retain());
        } else {
            sendRowsEvent(value);
        }
        return noRowFiltered;
    }

    private void sendRowsEvent(OutboundLogEventContext value) {
        value.getRowsEvent().write(new IoCache() {
            @Override
            public void write(byte[] data) {
            }

            @Override
            public void write(Collection<ByteBuf> byteBufs) {
                List<ByteBuf> bufs = (List<ByteBuf>) byteBufs;
                int bufSize = bufs.size();
                for (int i = 0; i < bufSize; i += 2) {
                    bufs.get(i).readerIndex(0);
                    ByteBuf send;
                    if (i + 1 < bufSize) {
                        bufs.get(i + 1).readerIndex(0);
                        send = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer().addComponents(true, bufs.get(i), bufs.get(i + 1));
                    } else {
                        send = bufs.get(i);
                    }
                    ChannelFuture future = channel.writeAndFlush(send);
                    future.addListener((GenericFutureListener) f -> {
                        if (!f.isSuccess()) {
                            channel.close();
                            logger.error("[Send] {} error", channel, f.cause());
                        }
                    });
                }
            }

            @Override
            public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
            }

            @Override
            public void write(LogEvent logEvent) {
            }
        });
    }
}
