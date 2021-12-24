package com.ctrip.framework.drc.core.driver.binlog.header;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.Packet;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;

/**
 * Created by @author zhuYongMing on 2019/9/5.
 * see https://dev.mysql.com/doc/internals/en/binlog-event-header.html
 */
public class LogEventHeader implements Packet<LogEventHeader>, Releasable {

    private long eventTimestamp;

    private int eventType;

    private long serverId;

    private long eventSize;

    private long nextEventStartPosition;

    private int flags;

    private ByteBuf headerBuf;


    public LogEventHeader() {

    }

    public LogEventHeader(final int eventType, final long serverId,
                          final long eventSize, final long nextEventStartPosition) {
        this(eventType, serverId, eventSize, nextEventStartPosition, 0);
    }

    public LogEventHeader(final int eventType, final long serverId,
                          final long eventSize, final long nextEventStartPosition, int flags) {
        // valid params

        final long eventTimestamp = System.currentTimeMillis() / 1000;

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeUnsignedIntLittleEndian(eventTimestamp, out);
        ByteHelper.writeUnsignedByte(eventType, out);
        ByteHelper.writeUnsignedIntLittleEndian(serverId, out);
        ByteHelper.writeUnsignedIntLittleEndian(eventSize, out);
        ByteHelper.writeUnsignedIntLittleEndian(nextEventStartPosition, out);
        ByteHelper.writeUnsignedShortLittleEndian(flags, out);

        // review, why not PooledByteBufAllocator?
        this.headerBuf = PooledByteBufAllocator.DEFAULT.directBuffer(eventHeaderLengthVersionGt1).writeBytes(out.toByteArray());
        this.headerBuf.skipBytes(eventHeaderLengthVersionGt1);
        this.eventTimestamp = eventTimestamp; // 4bytes
        this.eventType = eventType; // 1byte
        this.serverId = serverId; // 4bytes
        this.eventSize = eventSize; // 4bytes
        this.nextEventStartPosition = nextEventStartPosition; // 4bytes
        this.flags = this.flags | flags; // 2bytes
    }

    @Override
    public LogEventHeader read(final ByteBuf byteBuf) {
        // if binlog version == 1, header length == 13; else if version > 1, header length == 19
        this.headerBuf = byteBuf.readRetainedSlice(eventHeaderLengthVersionGt1);
        this.eventTimestamp = headerBuf.readUnsignedIntLE(); // 4bytes
        this.eventType = (int) headerBuf.readUnsignedByte(); // 1byte
        this.serverId = headerBuf.readUnsignedIntLE(); // 4bytes
        this.eventSize = headerBuf.readUnsignedIntLE(); // 4bytes
        this.nextEventStartPosition = headerBuf.readUnsignedIntLE(); // 4bytes
        this.flags = headerBuf.readUnsignedShortLE(); // 2bytes

        // validate encode result
        validate();

        return this;
    }

    private void validate() {

    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {

    }

    @Override
    public void write(IoCache ioCache) {

    }

    @Override
    public void release() {
        if (null != headerBuf && headerBuf.refCnt() > 0) {
            // if can release, release it's all ref
            headerBuf.release(headerBuf.refCnt());
        }
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public int getEventType() {
        return eventType;
    }

    public long getServerId() {
        return serverId;
    }

    public long getEventSize() {
        return eventSize;
    }

    public long getNextEventStartPosition() {
        return nextEventStartPosition;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public ByteBuf getHeaderBuf() {
        return headerBuf;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public void setEventSize(long eventSize) {
        this.eventSize = eventSize;
    }
}
