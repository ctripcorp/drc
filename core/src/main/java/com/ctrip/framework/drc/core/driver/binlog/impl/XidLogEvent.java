package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;

/**
 * Created by @author zhuYongMing on 2019/9/12.
 */
public class XidLogEvent extends AbstractLogEvent {

    private long xid;

    private Long checksum;


    public XidLogEvent() {}

    public XidLogEvent(final long serverId, final long currentEventStartPosition, long xid) {

        // valid param
        this.xid = xid;
        this.checksum = 0L;

        // write
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeUnsignedInt64LittleEndian(getXid(), out);
        ByteHelper.writeUnsignedIntLittleEndian(getChecksum(), out);

        final byte[] payloadBytes = out.toByteArray();
        final int payloadLength = payloadBytes.length;

        // set logEventHeader
        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(
                new LogEventHeader(
                        xid_log_event.getType(), serverId, eventSize,
                        currentEventStartPosition + eventSize
                )
        );

        // set payload
        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(payloadBytes);
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);
    }

    @Override
    public XidLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        this.xid = payloadBuf.readLongLE(); // 8bytes
        this.checksum = readChecksumIfPossible(payloadBuf); // 4bytes
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }


    public long getXid() {
        return xid;
    }

    public Long getChecksum() {
        return checksum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XidLogEvent that = (XidLogEvent) o;
        return xid == that.xid &&
                Objects.equals(checksum, that.checksum);
    }

    @Override
    public int hashCode() {

        return Objects.hash(xid, checksum);
    }
}
