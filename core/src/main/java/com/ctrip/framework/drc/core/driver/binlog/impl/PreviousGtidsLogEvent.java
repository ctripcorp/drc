package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.previous_gtids_log_event;

/**
 * Created by @author zhuYongMing on 2019/9/14.
 */
public class PreviousGtidsLogEvent extends AbstractLogEvent {

    private GtidSet gtidSet;

    private Long checksum;


    public PreviousGtidsLogEvent() {}

    public PreviousGtidsLogEvent(final long serverId, final long currentEventStartPosition,
                                 final GtidSet gtidSet) throws IOException {
        // valid params
        // gtids bytes
        final byte[] bytes = gtidSet.encode();
        final int payloadLength = bytes.length + 4; // checksum length = 4

        // checksum bytes
        final long checksum = 0L;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeUnsignedIntLittleEndian(checksum, out);

        // set logEventHeader
        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(new LogEventHeader(
                previous_gtids_log_event.getType(), serverId, eventSize, currentEventStartPosition + eventSize)
        );

        // set payLoad
        // review, why not PooledByteBufAllocator?
        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(bytes).writeBytes(out.toByteArray());
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);

        this.gtidSet = gtidSet;
        this.checksum = checksum; // mock 0L, need calculate checksum
    }

    @Override
    public PreviousGtidsLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        final long uuidNumber = payloadBuf.readLongLE();
        Map<String, GtidSet.UUIDSet> uuidSets = new LinkedHashMap<>((int) uuidNumber);
        for (int i = 0; i < uuidNumber; i++) {
            // uuid
            final long high8 = payloadBuf.readLong();
            final long low8 = payloadBuf.readLong();
            final String uuid = new UUID(high8, low8).toString();

            // interval
            final long intervalNumber = payloadBuf.readLongLE();
            List<GtidSet.Interval> intervals = new ArrayList<>((int) intervalNumber);
            for (int j = 0; j < intervalNumber; j++) {
                final long start = payloadBuf.readLongLE();
                final long end = payloadBuf.readLongLE() - 1;
                intervals.add(new GtidSet.Interval(start, end));
            }

            uuidSets.put(uuid, new GtidSet.UUIDSet(uuid, intervals));
        }
        this.gtidSet = new GtidSet(uuidSets);
        this.checksum = payloadBuf.readUnsignedIntLE(); // 4bytes
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public GtidSet getGtidSet() {
        return gtidSet;
    }

    public Long getChecksum() {
        return checksum;
    }
}
