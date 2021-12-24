package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * Created by @author zhuYongMing on 2019/9/14.
 */
public class HeartBeatLogEvent extends AbstractLogEvent {

    private String identification; // binlog filename

    private Long checksum;

    @Override
    public HeartBeatLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        int maxIdentificationLength = (1 << 9) - 1;
        int remainingIdentificationLength = payloadBuf.writerIndex() - payloadBuf.readerIndex() - 4;
        int identifyLength = remainingIdentificationLength < maxIdentificationLength ? remainingIdentificationLength : maxIdentificationLength;
        byte[] identificationBytes = new byte[identifyLength];
        payloadBuf.readBytes(identificationBytes, 0, identifyLength);
        this.identification = new String(identificationBytes, ISO_8859_1); // fix length
        this.checksum = payloadBuf.readUnsignedIntLE(); // 4bytes
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public String getIdentification() {
        return identification;
    }

    public Long getChecksum() {
        return checksum;
    }
}
