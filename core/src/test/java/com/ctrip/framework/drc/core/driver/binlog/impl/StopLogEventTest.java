package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by @author zhuYongMing on 2019/9/29.
 */
public class StopLogEventTest {

    @Test
    public void readTest() {
        final ByteBuf byteBuf = initByteBuf();
        final StopLogEvent stopLogEvent = new StopLogEvent().read(byteBuf);
        if (null == stopLogEvent) {
            Assert.fail();
        }

        if (null == stopLogEvent.getLogEventHeader()) {
            Assert.fail();
        }

        Assert.assertEquals("1cc55e0b", Long.toHexString(stopLogEvent.getChecksum()));
    }

    /**
     * binlog:
     * # at 913
     * #190929 13:41:32 server id 1  end_log_pos 936 CRC32 0x1cc55e0b  Stop
     * SET @@SESSION.GTID_NEXT= 'AUTOMATIC' added by mysqlbinlog;
     * <p>
     * binary:
     * 00000391  0c 44 90 5d 03 01 00 00  00 17 00 00 00 a8 03 00  |.D.]............|
     * 000003a1  00 00 00 0b 5e c5 1c                              |....^..|
     */
    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(23);
        String hexString =
                "0c 44 90 5d 03 01 00 00  00 17 00 00 00 a8 03 00" +
                        "00 00 00 0b 5e c5 1c";
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
