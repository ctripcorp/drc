package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * Created by @author zhuYongMing on 2019/9/12.
 */
public class RotateLogEvent extends AbstractLogEvent {

    private long nextBinlogBeginPosition;

    private String nextBinlogName;

    private Long checksum;


    @Override
    public RotateLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        // if binlog version > 1, rotate event has position, now binlog version = 2, hardcode
        this.nextBinlogBeginPosition = payloadBuf.readLongLE(); // 8bytes

        int maxBinlogNameLength = (1 << 9) - 1; // 511
        int remainingBinlogNameLength = payloadBuf.writerIndex() - payloadBuf.readerIndex() - 4;
        int binlogNameLength = remainingBinlogNameLength < maxBinlogNameLength ? remainingBinlogNameLength : maxBinlogNameLength;
        byte[] binlogNameBytes = new byte[binlogNameLength];
        payloadBuf.readBytes(binlogNameBytes, 0, binlogNameLength);
        this.nextBinlogName = new String(binlogNameBytes, ISO_8859_1); // fix length

        this.checksum = payloadBuf.readUnsignedIntLE(); // 4bytes
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public long getNextBinlogBeginPosition() {
        return nextBinlogBeginPosition;
    }

    public String getNextBinlogName() {
        return nextBinlogName;
    }

    public Long getChecksum() {
        return checksum;
    }
}
