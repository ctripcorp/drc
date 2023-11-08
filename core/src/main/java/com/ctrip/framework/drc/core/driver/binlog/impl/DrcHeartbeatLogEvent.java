package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_heartbeat_log_event;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.TIME_SPAN_MS;

/**
 * Created by mingdongli
 * 2019/11/25 下午10:34.
 */
public class DrcHeartbeatLogEvent extends AbstractLogEvent implements LogEventMerger {

    public static final int APPLIER_TOUCH_PROGRESS = 0x01;

    public int code;

    private int flags;

    private long time = System.currentTimeMillis();

    public DrcHeartbeatLogEvent() {
    }

    public DrcHeartbeatLogEvent(int code) {
        this(code, 0);
    }

    public DrcHeartbeatLogEvent(int code, int flags) {
        this.code = code;
        this.flags = flags;
        byte[] body = toBytes();
        int payloadLength = body.length;
        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(new LogEventHeader(drc_heartbeat_log_event.getType(), 0, eventSize, eventSize));

        // set payLoad
        // review, why not PooledByteBufAllocator?
        final ByteBuf payloadByteBuf = ByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(body);
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);
    }

    @Override
    public DrcHeartbeatLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        this.code = payloadBuf.readUnsignedShortLE();
        this.flags = payloadBuf.readUnsignedShortLE();
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
        ByteHelper.writeUnsignedShortLittleEndian(code, out);
        ByteHelper.writeUnsignedShortLittleEndian(flags, out);
        return out.toByteArray();
    }

    public int getCode() {
        return code;
    }

    public boolean shouldTouchProgress(){
        return (this.flags & APPLIER_TOUCH_PROGRESS) == APPLIER_TOUCH_PROGRESS;
    }

    public boolean timeValid() {
        return System.currentTimeMillis() - time < TIME_SPAN_MS;
    }
}
