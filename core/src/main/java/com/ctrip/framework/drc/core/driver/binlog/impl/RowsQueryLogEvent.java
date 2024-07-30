package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;

/**
 * Created by @author zhuYongMing on 2019/9/28.
 */
public class RowsQueryLogEvent extends AbstractLogEvent {

    private String rowsQuery;

    private Long checksum;


    @Override
    public RowsQueryLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        payloadBuf.readUnsignedByte(); // readIndex + 1
        final int length = payloadBuf.writerIndex() - payloadBuf.readerIndex() - 4;
        this.rowsQuery = readFixLengthStringDefaultCharset(payloadBuf, length);
        this.checksum = readChecksumIfPossible(payloadBuf); // 4bytes
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public String getRowsQuery() {
        return rowsQuery;
    }

    public Long getChecksum() {
        return checksum;
    }
}
