package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_ddl_log_event;
import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.LOG_EVENT_IGNORABLE_F;

/**
 * @Author limingdong
 * @create 2020/3/11
 */
public class DrcDdlLogEvent extends AbstractLogEvent {

    protected String schema;

    protected String ddl;

    public DrcDdlLogEvent() {
    }

    public DrcDdlLogEvent(String schema, String ddl, int serverId, long currentEventStartPosition) throws IOException {
        this(schema, ddl, serverId, currentEventStartPosition, drc_ddl_log_event);
    }

    public DrcDdlLogEvent(String schema, String ddl, int serverId, long currentEventStartPosition, LogEventType logEventType) throws IOException {
        this.schema = schema;
        this.ddl = ddl;

        final byte[] payloadBytes = payloadToBytes();
        final int payloadLength = payloadBytes.length;

        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(new LogEventHeader(logEventType.getType(), serverId, eventSize, currentEventStartPosition + eventSize, LOG_EVENT_IGNORABLE_F));

        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(payloadBytes);
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);
    }

    protected byte[] payloadToBytes() {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();


        byte[] schemaBytes = schema.getBytes();
        ByteHelper.writeUnsignedIntLittleEndian(schemaBytes.length, out);
        ByteHelper.writeFixedLengthBytesFromStart(schema.getBytes(), schemaBytes.length, out);

        byte[] ddlBytes = ddl.getBytes();
        ByteHelper.writeUnsignedIntLittleEndian(ddlBytes.length, out);
        ByteHelper.writeFixedLengthBytesFromStart(ddlBytes, ddlBytes.length, out);

        return out.toByteArray();
    }

    @Override
    public DrcDdlLogEvent read(ByteBuf byteBuf) {
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

        return this;
    }

    protected LogEvent readHeader(ByteBuf byteBuf){
        return super.read(byteBuf);
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public String getDdl() {
        return ddl;
    }

    public String getSchema() {
        return schema;
    }

}
