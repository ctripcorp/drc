package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by @author zhuYongMing on 2019/9/28.
 */
public class RowsQueryLogEventTest {

    @Test
    public void readTest() {
        final RowsQueryLogEvent rowsQueryLogEvent = new RowsQueryLogEvent().read(initByteBuf());
        Assert.assertEquals(
                "/*480161*/\r\nUPDATE `order_serverruncontrol` SET `biztype`=11,`ip`='10.15.106.251',`runtime`='2020-08-26 23:51:18.313734',`runstatus`='S'," +
                        "`operator`='System',`operatetime`='2020-08-26 23:51:18.313734',`datachange_lasttime`='2020-08-26 23:51:18.313734' WHERE `id`=2909",
                rowsQueryLogEvent.getRowsQuery()
        );
        Assert.assertEquals("5da7add5", Long.toHexString(rowsQueryLogEvent.getChecksum()));

    }

    /**
     * rows query : update `dalservice`.`user_info` set `enabled`='0' where `id`='4'
     */
    private ByteBuf initByteBuf() {
        final String hexString = "f6 84 46 5f 1d 0b 39 01  00 22 01 00 00 f9 22 03" +
                "0a 80 00 0a 2f 2a 34 38  30 31 36 31 2a 2f 0d 0a" +
                "55 50 44 41 54 45 20 60  6f 72 64 65 72 5f 73 65" +
                "72 76 65 72 72 75 6e 63  6f 6e 74 72 6f 6c 60 20" +
                "53 45 54 20 60 62 69 7a  74 79 70 65 60 3d 31 31" +
                "2c 60 69 70 60 3d 27 31  30 2e 31 35 2e 31 30 36" +
                "2e 32 35 31 27 2c 60 72  75 6e 74 69 6d 65 60 3d" +
                "27 32 30 32 30 2d 30 38  2d 32 36 20 32 33 3a 35" +
                "31 3a 31 38 2e 33 31 33  37 33 34 27 2c 60 72 75" +
                "6e 73 74 61 74 75 73 60  3d 27 53 27 2c 60 6f 70" +
                "65 72 61 74 6f 72 60 3d  27 53 79 73 74 65 6d 27" +
                "2c 60 6f 70 65 72 61 74  65 74 69 6d 65 60 3d 27" +
                "32 30 32 30 2d 30 38 2d  32 36 20 32 33 3a 35 31" +
                "3a 31 38 2e 33 31 33 37  33 34 27 2c 60 64 61 74" +
                "61 63 68 61 6e 67 65 5f  6c 61 73 74 74 69 6d 65" +
                "60 3d 27 32 30 32 30 2d  30 38 2d 32 36 20 32 33" +
                "3a 35 31 3a 31 38 2e 33  31 33 37 33 34 27 20 57" +
                "48 45 52 45 20 60 69 64  60 3d 32 39 30 39 d5 ad" +
                "a7 5d";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
