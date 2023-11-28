package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_filter_log_event;

/**
 * Created by jixinwang on 2023/11/20
 */
public class FilterLogEvent extends AbstractLogEvent {

    public static String UNKNOWN = "unknown";

    private String schemaName = UNKNOWN;

    private long nextTransactionOffset = 0;

    public FilterLogEvent() {
    }

    public void encode(String schemaName, long nextTransactionOffset) {
        if (schemaName == null) {
            schemaName = UNKNOWN;
        }
        this.schemaName = schemaName;
        this.nextTransactionOffset = nextTransactionOffset;
        final byte[] payloadBytes = payloadToBytes();
        final int payloadLength = payloadBytes.length;

        // set logEventHeader
        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(new LogEventHeader(drc_filter_log_event.getType(), 0, eventSize, nextTransactionOffset)
        );

        // set payload
        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(payloadBytes);
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);
    }

    private byte[] payloadToBytes() {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            ByteHelper.writeVariablesLengthStringDefaultCharset(schemaName, out);
        } catch (IOException e) {
            throw new RuntimeException("FilterLogEvent payloadToByte error");
        }
        ByteHelper.writeIntLittleEndian((int) nextTransactionOffset, out);
        return out.toByteArray();
    }

    @Override
    public LogEventType getLogEventType() {
        return drc_filter_log_event;
    }

    @Override
    public FilterLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        this.schemaName = readVariableLengthStringDefaultCharset(payloadBuf);
        this.nextTransactionOffset = payloadBuf.readUnsignedIntLE();
        return this;
    }

    public long getNextTransactionOffset() {
        return nextTransactionOffset;
    }

    public String getSchemaName() {
        return schemaName;
    }
}
