package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.LOG_EVENT_IGNORABLE_F;

/**
 * @Author limingdong
 * @create 2020/12/31
 */
public abstract class DrcGenericLogEvent<T> extends AbstractLogEvent {

    private T content;

    public DrcGenericLogEvent() {
    }

    public DrcGenericLogEvent(T content, int serverId, long currentEventStartPosition, LogEventType logEventType) {

        byte[] bytes = Codec.DEFAULT.encodeAsBytes(content);

        final int payloadLength = bytes.length;

        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(new LogEventHeader(logEventType.getType(), serverId, eventSize, currentEventStartPosition + eventSize, LOG_EVENT_IGNORABLE_F));

        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(bytes);
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);

        this.content = content;
    }

    @Override
    public DrcGenericLogEvent<T> read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        byte[] bytes = new byte[payloadBuf.readableBytes()];
        payloadBuf.readBytes(bytes);
        content = JsonCodec.INSTANCE.decode(bytes, getGenericTypeReference());
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public T getContent() {
        return content;
    }

    protected abstract GenericTypeReference<T> getGenericTypeReference();
}
