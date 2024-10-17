package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_filter_log_event;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_DB_DELAY_MONITOR_TABLE_NAME_PREFIX;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MONITOR_SCHEMA_NAME;

/**
 * Created by jixinwang on 2023/11/20
 */
public class FilterLogEvent extends AbstractLogEvent {

    public static final String UNKNOWN = "unknown";

    private String schemaName = UNKNOWN;
    private String lastTableName = UNKNOWN;
    private int eventCount = 0;
    private Integer rowsEventCount = null;
    private long nextTransactionOffset = 0;

    public FilterLogEvent() {
    }

    public void encode(String schemaName, String lastTableName, int eventCount, Integer rowsEventCount, long nextTransactionOffset) {
        if (schemaName == null) {
            schemaName = UNKNOWN;
        }
        if (lastTableName == null) {
            lastTableName = UNKNOWN;
        }
        this.schemaName = schemaName;
        this.nextTransactionOffset = nextTransactionOffset;
        this.lastTableName = lastTableName;
        this.eventCount = eventCount;
        this.rowsEventCount = rowsEventCount;
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
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            ByteHelper.writeVariablesLengthStringDefaultCharset(schemaName, out);
            ByteHelper.writeIntLittleEndian((int) nextTransactionOffset, out);
            ByteHelper.writeVariablesLengthStringDefaultCharset(lastTableName, out);
            ByteHelper.writeIntLittleEndian(eventCount, out);
            if (rowsEventCount != null) {
                ByteHelper.writeIntLittleEndian(rowsEventCount, out);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("FilterLogEvent payloadToByte error", e);
        }
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
        this.lastTableName = readVariableLengthStringDefaultCharset(payloadBuf);
        this.eventCount = (int) payloadBuf.readUnsignedIntLE();
        if (hasRemaining(payloadBuf)) {
            this.rowsEventCount = (int) payloadBuf.readUnsignedIntLE();
        }
        return this;
    }

    @Override
    public boolean hasRemaining(ByteBuf payloadBuf) {
        return payloadBuf.readableBytes() >= 4;
    }

    public long getNextTransactionOffset() {
        return nextTransactionOffset;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getSchemaNameLowerCaseV2() {
        String schemaNameLowerCase = schemaName.toLowerCase();
        if (DRC_MONITOR_SCHEMA_NAME.equals(schemaNameLowerCase)) {
            String lastTableNameLowerCase = lastTableName.toLowerCase();
            if (lastTableNameLowerCase.startsWith(DRC_DB_DELAY_MONITOR_TABLE_NAME_PREFIX)
                    && lastTableNameLowerCase.length() > DRC_DB_DELAY_MONITOR_TABLE_NAME_PREFIX.length()) {
                return lastTableNameLowerCase.substring(DRC_DB_DELAY_MONITOR_TABLE_NAME_PREFIX.length());
            }
        }
        return schemaNameLowerCase;
    }

    public String getLastTableName() {
        return lastTableName;
    }

    public int getEventCount() {
        return eventCount;
    }

    public boolean isNoRowsEvent() {
        return rowsEventCount != null && rowsEventCount == 0;
    }

    @VisibleForTesting
    public Integer getRowsEventCount() {
        return rowsEventCount;
    }
}
