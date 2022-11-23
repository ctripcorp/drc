package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.ByteArrayOutputStream;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_ddl_log_event;
import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.LOG_EVENT_IGNORABLE_F;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-12-30
 */
public class ParsedDdlLogEvent extends DrcDdlLogEvent {

    private QueryType queryType;

    private String table = "";

    public ParsedDdlLogEvent() {
    }

    public ParsedDdlLogEvent(String schema, String table, String ddl, QueryType queryType) {
        this.schema = schema;
        this.ddl = ddl;
        if (table != null) {
            this.table = table;
        }
        this.queryType = queryType;

        final byte[] payloadBytes = payloadToBytes();
        final int payloadLength = payloadBytes.length;

        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(new LogEventHeader(drc_ddl_log_event.getType(), 0, eventSize,  eventSize, LOG_EVENT_IGNORABLE_F));

        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(payloadBytes);
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);
    }

    protected byte[] payloadToBytes() {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] superBody = super.payloadToBytes();
        ByteHelper.writeFixedLengthBytesFromStart(superBody, superBody.length, out);

        byte[] tableBytes = table.getBytes();
        ByteHelper.writeUnsignedIntLittleEndian(tableBytes.length, out);
        ByteHelper.writeFixedLengthBytesFromStart(tableBytes, tableBytes.length, out);

        int type = queryType.ordinal();
        ByteHelper.writeUnsignedShortLittleEndian(type, out);

        return out.toByteArray();
    }

    @Override
    public ParsedDdlLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = readHeader(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        byte[] dst = new byte[payloadBuf.readableBytes()];
        payloadBuf.readBytes(dst, 0, payloadBuf.readableBytes());

        int index = 0;
        long schemeLength = ByteHelper.readUnsignedIntLittleEndian(dst, index);
        index += 4;

        byte[] schemaBytes = ByteHelper.readFixedLengthBytes(dst, index, (int) schemeLength);
        schema = new String(schemaBytes);
        index += schemaBytes.length;

        long ddlLength = ByteHelper.readUnsignedIntLittleEndian(dst, index);
        index += 4;

        byte[] ddlBytes = ByteHelper.readFixedLengthBytes(dst, index, (int) ddlLength);
        ddl = new String(ddlBytes);
        index += ddlBytes.length;

        long tableLength = ByteHelper.readUnsignedIntLittleEndian(dst, index);
        index += 4;

        byte[] tableBytes = ByteHelper.readFixedLengthBytes(dst, index, (int) tableLength);
        table = new String(tableBytes);
        index += tableBytes.length;

        int type = ByteHelper.readUnsignedShortLittleEndian(dst, index);
        this.queryType = QueryType.getQueryType(type);
        return this;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public String getTable() {
        return table;
    }
}
