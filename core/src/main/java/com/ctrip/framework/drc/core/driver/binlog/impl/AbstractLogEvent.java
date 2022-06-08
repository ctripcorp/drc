package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.AsciiString;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.BitSet;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.NULL_TERMINATED_STRING_DELIMITER;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public abstract class AbstractLogEvent implements LogEvent {

    private LogEventHeader logEventHeader;

    private ByteBuf payloadBuf;

    @Override
    public LogEvent read(ByteBuf byteBuf) {
        if (null == logEventHeader) {
            // first time read
            this.logEventHeader = new LogEventHeader().read(byteBuf);
            final int eventSize = (int) logEventHeader.getEventSize();
            final int readLength = eventSize - eventHeaderLengthVersionGt1;
            if (byteBuf.isReadable(readLength)) {
                // buf is enough byteBuf to construct current logEvent payload
                payloadBuf = byteBuf.readRetainedSlice(readLength);
                return this;
            } else {
                // not enough
                payloadBuf = byteBuf.readRetainedSlice(byteBuf.readableBytes());
                return null;
            }
        } else {
            // read again
            final int eventSize = (int) logEventHeader.getEventSize();
            final int readLength = eventSize - eventHeaderLengthVersionGt1 - payloadBuf.writerIndex();
            if (byteBuf.isReadable(readLength)) {
                // is enough
                final ByteBuf readBuf = byteBuf.readRetainedSlice(readLength);
                payloadBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer().addComponents(true, payloadBuf, readBuf);
                return this;
            } else {
                // not enough again
                final ByteBuf readBuf = byteBuf.readRetainedSlice(byteBuf.readableBytes());
                payloadBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer().addComponents(true, payloadBuf, readBuf);
                return null;
            }
        }
    }

    @Override
    public void write(ByteBuf byteBuf) {
        //do read logevent header
    }

    @Override
    public void write(IoCache ioCache) {
        ByteBuf headByteBuf;
        if (null != logEventHeader) {
            headByteBuf = logEventHeader.getHeaderBuf();
        } else {
            throw new IllegalStateException("haven’t init this event, can’t start write.");
        }

        if (null != payloadBuf && null != headByteBuf) {
            ioCache.write(Lists.newArrayList(PooledByteBufAllocator.DEFAULT.compositeDirectBuffer().addComponents(true, headByteBuf, payloadBuf))); // write header and payload
        } else {
            throw new IllegalStateException("haven’t init this event, can’t start write.");
        }
    }

    @Override
    public void release() {
        // if can release, release it's all ref
        if (null != logEventHeader) {
            logEventHeader.release();
        }

        if (null != payloadBuf && payloadBuf.refCnt() > 0) {
            payloadBuf.release(payloadBuf.refCnt());
        }
    }

    @Override
    public LogEventType getLogEventType() {
        return LogEventType.getLogEventType(getLogEventHeader().getEventType());
    }

    @Override
    public boolean isEmpty() {
        return null == getLogEventHeader() && null == payloadBuf;
    }

    @Override
    public LogEventHeader getLogEventHeader() {
        return this.logEventHeader;
    }

    public void setLogEventHeader(final LogEventHeader logEventHeader) {
        this.logEventHeader = logEventHeader;
    }

    public ByteBuf getPayloadBuf() {
        return payloadBuf;
    }

    protected void setPayloadBuf(final ByteBuf payloadBuf) {
        this.payloadBuf = payloadBuf;
    }

    /**
     * see https://dev.mysql.com/doc/internals/en/integer.html
     * If it is < 0xfb, treat it as a 1-byte integer.
     * If it is 0xfc, it is followed by a 2-byte integer.
     * If it is 0xfd, it is followed by a 3-byte integer.
     * If it is 0xfe, it is followed by a 8-byte integer.
     * <p>
     * todo
     * Caution:
     * If the first byte of a packet is a length-encoded integer and its byte value is 0xfe,
     * you must check the length of the packet to verify that it has enough space for a 8-byte integer.
     * If not, it may be an EOF_Packet instead.
     * <p>
     * If it is 0xfb, it is represents a NULL in a ProtocolText::ResultsetRow.
     * <p>
     * If it is 0xff and is the first byte of an ERR_Packet
     * Caution:
     * 0xff as the first byte of a length-encoded integer is undefined.
     */
    protected long readLengthEncodeInt(final ByteBuf byteBuf) {
        final int encodeLength = byteBuf.readUnsignedByte();
        if (encodeLength < 251) {
            return encodeLength;
        }

        switch (encodeLength) {
            case 251:
                return NULL_TERMINATED_STRING_DELIMITER;
            case 252:
                return byteBuf.readUnsignedShortLE(); // 2bytes
            case 253:
                return byteBuf.readUnsignedMediumLE(); // 3bytes
            case 254:
                // TODO: 2019/9/11 may be ERR_Packet
                return byteBuf.readLongLE(); // 8bytes
            default:
                // TODO: 2019/9/8 ERR_Packet
                return 0;
        }
    }

    protected BitSet readBitSet(final ByteBuf byteBuf, final long length) {
        BitSet bitSet = new BitSet((int) length);

        for (int bit = 0; bit < length; bit += 8) {
            final short flag = byteBuf.readUnsignedByte();
            if (flag == 0) {
                continue;
            }
            if ((flag & 0x01) != 0) bitSet.set(bit);
            if ((flag & 0x02) != 0) bitSet.set(bit + 1);
            if ((flag & 0x04) != 0) bitSet.set(bit + 2);
            if ((flag & 0x08) != 0) bitSet.set(bit + 3);
            if ((flag & 0x10) != 0) bitSet.set(bit + 4);
            if ((flag & 0x20) != 0) bitSet.set(bit + 5);
            if ((flag & 0x40) != 0) bitSet.set(bit + 6);
            if ((flag & 0x80) != 0) bitSet.set(bit + 7);
        }

        return bitSet;
    }

    protected void writeBitSet(BitSet bitSet, long length, ByteArrayOutputStream out) {
        for (int bit = 0; bit < length; bit += 8) {
            short flag = 0;
            if (bitSet.get(bit))     flag |= 0x01;
            if (bitSet.get(bit + 1)) flag |= 0x02;
            if (bitSet.get(bit + 2)) flag |= 0x04;
            if (bitSet.get(bit + 3)) flag |= 0x08;
            if (bitSet.get(bit + 4)) flag |= 0x10;
            if (bitSet.get(bit + 5)) flag |= 0x20;
            if (bitSet.get(bit + 6)) flag |= 0x40;
            if (bitSet.get(bit + 7)) flag |= 0x80;
            ByteHelper.writeUnsignedByte(flag, out);
        }
    }

    protected String readVariableLengthStringDefaultCharset(final ByteBuf byteBuf) {
        // mysql default charset is latin1, map to java is IOS_8859_1
        final short length = byteBuf.readUnsignedByte();
        return readFixLengthStringDefaultCharset(byteBuf, length);
    }

    protected String readVariableLengthString(final ByteBuf byteBuf, final Charset charset) {
        final short length = byteBuf.readUnsignedByte();
        return readFixLengthString(byteBuf, length, charset);
    }

    protected String readFixLengthStringDefaultCharset(final ByteBuf byteBuf, final int length) {
        // mysql default charset is latin1, map to java is IOS_8859_1
        return readFixLengthString(byteBuf, length, ISO_8859_1);
    }

    protected String readFixLengthString(final ByteBuf byteBuf, final int length, final Charset charset) {
        final CharSequence string = byteBuf.readCharSequence(length, charset);

        if (string instanceof AsciiString) {
            final AsciiString asciiString = (AsciiString) string;
            return asciiString.toString();
        }
        return string.toString();
    }

    protected boolean hasRemaining(final ByteBuf payloadBuf) {
        return payloadBuf.readableBytes() > 4; // 4 is checksum length
    }
}
