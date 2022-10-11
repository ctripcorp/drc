package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.gtid_log_event;
import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.LOG_EVENT_IGNORABLE_F;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.*;

/**
 * Created by @author zhuYongMing on 2019/9/11.
 */
public class GtidLogEvent extends AbstractLogEvent {

    private boolean commitFlag;

    private UUID serverUUID;

    private long id;

    private long lastCommitted;

    private long sequenceNumber;

    private long nextTransactionOffset = 0;

    private Long checksum;

    public GtidLogEvent() {
    }

    @Override
    public GtidLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        this.commitFlag = payloadBuf.readUnsignedByte() != 0;

        final long high8 = payloadBuf.readLong();
        final long low8 = payloadBuf.readLong();
        this.serverUUID = new UUID(high8, low8);

        this.id = payloadBuf.readLongLE();
        if (payloadBuf.readUnsignedByte() == 2) { // hardcode
            this.lastCommitted = payloadBuf.readLongLE();
            this.sequenceNumber = payloadBuf.readLongLE();
        }

        if (hasRemaining(payloadBuf)) {
            nextTransactionOffset = payloadBuf.readUnsignedIntLE();
        }

        this.checksum = payloadBuf.readUnsignedIntLE(); // 4bytes
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public boolean isCommitFlag() {
        return commitFlag;
    }

    public UUID getServerUUID() {
        return serverUUID;
    }

    public long getId() {
        return id;
    }

    public String getGtid() {
        StringBuilder stringBuilder = new StringBuilder(64);
        return stringBuilder.append(serverUUID.toString()).append(":").append(id).toString();
    }

    public long getLastCommitted() {
        return lastCommitted;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public Long getChecksum() {
        return checksum;
    }

    public void setEventType(int eventType) {
        LogEventHeader logEventHeader = getLogEventHeader();
        int flags = logEventHeader.getFlags() | LOG_EVENT_IGNORABLE_F;
        logEventHeader.setFlags(flags);
        ByteBuf headerBuf = logEventHeader.getHeaderBuf();
        logEventHeader.setEventType(eventType);
        int previousWriterIndex = headerBuf.writerIndex();
        int previousReaderIndex = headerBuf.readerIndex();
        headerBuf.readerIndex(0);

        headerBuf.writerIndex(4);
        headerBuf.writeBytes(new byte[] {(byte) eventType});

        headerBuf.writerIndex(eventHeaderLengthVersionGt1 - 2);
        headerBuf.writeShortLE(flags);

        headerBuf.writerIndex(previousWriterIndex);
        headerBuf.readerIndex(previousReaderIndex);
    }

    @VisibleForTesting
    public void setServerUUID(UUID serverUUID) {
        this.serverUUID = serverUUID;
    }

    public long getNextTransactionOffset() {
        return nextTransactionOffset;
    }

    public void setNextTransactionOffsetAndUpdateEventSize(long nextTransactionOffset) {
        this.nextTransactionOffset = nextTransactionOffset;

        LogEventHeader logEventHeader = getLogEventHeader();
        long originEventSize = logEventHeader.getEventSize();
        ByteBuf headerBuf = logEventHeader.getHeaderBuf();
        int previousWriterIndex = headerBuf.writerIndex();
        int previousReaderIndex = headerBuf.readerIndex();
        headerBuf.readerIndex(0);

        headerBuf.writerIndex(EVENT_LEN_OFFSET);
        long newEventSize = originEventSize + BINLOG_TRANSACTION_OFFSET_LENGTH;
        logEventHeader.setEventSize(newEventSize);
        headerBuf.writeIntLE((int) newEventSize);
        headerBuf.writerIndex(previousWriterIndex);
        headerBuf.readerIndex(previousReaderIndex);

        ByteBuf payloadBuf = getPayloadBuf();
        int capacity = payloadBuf.capacity();

        payloadBuf.readerIndex(0);
        ByteBuf postHeader = payloadBuf.readRetainedSlice(capacity - BINLOG_CHECKSUM_LENGTH);  //BINLOG_CHECKSUM_LENGTH bytes for checksum
        ByteBuf realPayload = payloadBuf.readRetainedSlice(BINLOG_CHECKSUM_LENGTH);

        final ByteBuf nextTransactionOffsetByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(BINLOG_TRANSACTION_OFFSET_LENGTH);
        nextTransactionOffsetByteBuf.writeIntLE((int) nextTransactionOffset);

        ByteBuf modifiedPayload =  Unpooled.wrappedBuffer(postHeader, nextTransactionOffsetByteBuf, realPayload).slice();
        modifiedPayload.skipBytes(modifiedPayload.readableBytes());
        setPayloadBuf(modifiedPayload);
        ReferenceCountUtil.release(payloadBuf);
    }

    @VisibleForTesting
    public GtidLogEvent(String gtid) {
        if (gtid == null) {
            throw new IllegalArgumentException("gtid is null");
        }
        String[] uuidAndSequenceNumber = gtid.split(":");
        if (uuidAndSequenceNumber.length != 2) {
            throw new IllegalArgumentException("gtid format error");
        }
        this.id = Long.parseLong(uuidAndSequenceNumber[1]);
        this.checksum = 0L;
        this.commitFlag = true;
        this.serverUUID = UUID.fromString(uuidAndSequenceNumber[0]);
        this.sequenceNumber = -1L;
        this.lastCommitted = -1L;

        final byte[] payloadBytes = payloadToBytes();
        final int payloadLength = payloadBytes.length;

        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(new LogEventHeader(gtid_log_event.getType(), this.id, eventSize, eventSize));

        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(payloadBytes);
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);
    }

    private byte[] payloadToBytes() {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        ByteHelper.writeUnsignedByte(this.commitFlag ? 1 : 0, out);

        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(this.serverUUID.getMostSignificantBits());
        bb.putLong(this.serverUUID.getLeastSignificantBits());
        try {
            out.write(bb.array());
        } catch (IOException e) {
        }

        ByteHelper.writeUnsignedInt64LittleEndian(this.id, out);
        ByteHelper.writeUnsignedByte(2, out);
        ByteHelper.writeUnsignedInt64LittleEndian(this.lastCommitted, out);
        ByteHelper.writeUnsignedInt64LittleEndian(this.sequenceNumber, out);
        ByteHelper.writeUnsignedIntLittleEndian(this.checksum, out);
        return out.toByteArray();
    }
}