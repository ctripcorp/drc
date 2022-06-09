package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_error_log_event;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * Created by mingdongli
 * 2019/10/23 上午9:27.
 */
public class DrcErrorLogEvent extends AbstractLogEvent implements LogEventMerger {

    public int errorNumber;

    public String message;

    public DrcErrorLogEvent() {
    }

    public DrcErrorLogEvent(int errorNumber, String message) {
        this.errorNumber = errorNumber;
        this.message = message;
        byte[] body = toBytes();
        int payloadLength = body.length;
        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(new LogEventHeader(drc_error_log_event.getType(), 0, eventSize, eventSize));

        // set payLoad
        // review, why not PooledByteBufAllocator?
        final ByteBuf payloadByteBuf = ByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(body);
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);
    }

    @Override
    public DrcErrorLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        this.errorNumber = payloadBuf.readUnsignedShortLE(); // 4bytes
        this.message = readFixLengthStringDefaultCharset(payloadBuf, payloadBuf.readableBytes());
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    @Override
    protected List<ByteBuf> getEventByteBuf(ByteBuf headByteBuf, ByteBuf payloadBuf) {
        return mergeByteBuf(headByteBuf, payloadBuf);
    }

    private byte[] toBytes() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeUnsignedShortLittleEndian(errorNumber, out);
        byte[] messageBytes = message.getBytes(ISO_8859_1);
        try {
            out.write(messageBytes);
        } catch (IOException ignored) {
            //
        }
        return out.toByteArray();
    }

    public int getErrorNumber() {
        return errorNumber;
    }

    public String getMessage() {
        return message;
    }
}
