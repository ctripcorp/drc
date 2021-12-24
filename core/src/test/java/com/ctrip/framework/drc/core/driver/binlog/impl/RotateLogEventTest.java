package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.rotate_log_event;

/**
 * Created by @author zhuYongMing on 2019/9/12.
 */
public class RotateLogEventTest {

    @Test
    public void readTest() {
        final ByteBuf byteBuf = initByteBuf();
        final RotateLogEvent rotateLogEvent = new RotateLogEvent().read(byteBuf);
        if (null == rotateLogEvent) {
            Assert.fail();
        }

        if (null == rotateLogEvent.getLogEventHeader()) {
            Assert.fail();
        }

        Assert.assertEquals(rotate_log_event, LogEventType.getLogEventType(rotateLogEvent.getLogEventHeader().getEventType()));
        Assert.assertEquals(4, rotateLogEvent.getNextBinlogBeginPosition());
        Assert.assertEquals("mysql-bin.000018", rotateLogEvent.getNextBinlogName());
        Assert.assertEquals("8706375e", Long.toHexString(rotateLogEvent.getChecksum()));

        Assert.assertEquals(47, byteBuf.readerIndex());
        Assert.assertEquals(28, rotateLogEvent.getPayloadBuf().readerIndex());
        Assert.assertEquals(19, rotateLogEvent.getLogEventHeader().getHeaderBuf().readerIndex());
    }

//    @Test
//    public void readLazyTest() {
//        final ByteBuf byteBuf = initByteBuf();
//        final RotateLogEvent rotateLogEvent = new RotateLogEvent().read(byteBuf);
//        if (null == rotateLogEvent) {
//            Assert.fail();
//        }
//
//        if (null == rotateLogEvent.getLogEventHeader()) {
//            Assert.fail();
//        }
//
//        Assert.assertEquals(0, rotateLogEvent.getNextBinlogBeginPosition());
//        Assert.assertNull(rotateLogEvent.getNextBinlogName());
//        Assert.assertNull(rotateLogEvent.getChecksum());
//
//        Assert.assertEquals(47, byteBuf.readerIndex());
//        Assert.assertEquals(0, rotateLogEvent.getPayloadBuf().readerIndex());
//        Assert.assertEquals(19, rotateLogEvent.getLogEventHeader().getHeaderBuf().readerIndex());
//    }

    /**
     * # at 4031
     * #190830 12:00:07 server id 1  end_log_pos 4078 CRC32 0x8706375e         Rotate to mysql-bin.000018  pos: 4
     *  47 9f 68 5d 04 01 00 00  00 2f 00 00 00 ee 0f 00
     *  00 00 00 04 00 00 00 00  00 00 00 6d 79 73 71 6c
     *  2d 62 69 6e 2e 30 30 30  30 31 38 5e 37 06 87
     */
    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(47);
        byte[] bytes = new byte[] {
                (byte) 0x47, (byte) 0x9f, (byte) 0x68, (byte) 0x5d, (byte) 0x04, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x2f, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xee, (byte) 0x0f, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x04, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x6d, (byte) 0x79, (byte) 0x73, (byte) 0x71, (byte) 0x6c,

                (byte) 0x2d, (byte) 0x62, (byte) 0x69, (byte) 0x6e, (byte) 0x2e, (byte) 0x30, (byte) 0x30, (byte) 0x30,
                (byte) 0x30, (byte) 0x31, (byte) 0x38, (byte) 0x5e, (byte) 0x37, (byte) 0x06, (byte) 0x87
        };

        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

}
