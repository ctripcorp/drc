package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by @author zhuYongMing on 2019/9/14.
 */
public class HeartBeatLogEventTest {

    @Test
    public void readTest() {
        final ByteBuf byteBuf = initByteBuf();
        final HeartBeatLogEvent heartBeatLogEvent = new HeartBeatLogEvent().read(byteBuf);

        if (null == heartBeatLogEvent) {
            Assert.fail();
        }

        if (null == heartBeatLogEvent.getLogEventHeader()) {
            Assert.fail();
        }

        Assert.assertEquals(0, heartBeatLogEvent.getLogEventHeader().getHeaderBuf().readableBytes());
        Assert.assertEquals(0, heartBeatLogEvent.getPayloadBuf().readableBytes());
        Assert.assertEquals("mysql-bin.000001", heartBeatLogEvent.getIdentification());
        Assert.assertEquals("2297f290", Long.toHexString(heartBeatLogEvent.getChecksum()));
    }

    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(39);
        String decimalString = "0, 0, 0, 0, 27, 1, 0, 0, 0, 39, 0, 0, 0, 99, 109, 5, 0, 0, 0, 109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48, 48, 48, 48, 49, -112, -14, -105, 34";
        final byte[] bytes = BytesUtil.toBytesFromDecimalString(decimalString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
