package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;

/**
 * Created by @author zhuYongMing on 2019/9/29.
 */
public class StopLogEvent extends AbstractLogEvent {

    private Long checksum;


    @Override
    public StopLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        this.checksum = payloadBuf.readUnsignedIntLE(); // 4bytes
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public Long getChecksum() {
        return checksum;
    }
}
