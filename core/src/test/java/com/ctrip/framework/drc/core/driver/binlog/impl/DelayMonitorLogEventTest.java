package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-17
 */
public class DelayMonitorLogEventTest {

    private static final String gtid = "abcde123-5678-1234-abcd-abcd1234abcd:123456789";

    private DelayMonitorLogEvent delayMonitorLogEvent;

    @Before
    public void setUp() {
        ByteBuf eventByteBuf = initByteBuf();
        delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        delayMonitorLogEvent.setNeedReleased(true);
        delayMonitorLogEvent.setSrcDcName("fat");
        eventByteBuf.release();
    }

    @Test
    public void testRead() {
        ByteBuf headerBuf = delayMonitorLogEvent.getLogEventHeader().getHeaderBuf();
        headerBuf.readerIndex(0);

        ByteBuf payloadBuf = delayMonitorLogEvent.getPayloadBuf();
        payloadBuf.readerIndex(0);

        DelayMonitorLogEvent clone = new DelayMonitorLogEvent();

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headerBuf.readableBytes() + payloadBuf.readableBytes());
        compositeByteBuf.addComponents(true, headerBuf, payloadBuf);

        clone.read(compositeByteBuf);

        Assert.assertEquals(clone.getGtid(), delayMonitorLogEvent.getGtid());
        Assert.assertEquals(clone.getUpdateRowsEvent().getLogEventHeader().getHeaderBuf(), delayMonitorLogEvent.getUpdateRowsEvent().getLogEventHeader().getHeaderBuf());
        Assert.assertEquals(clone.getUpdateRowsEvent().getPayloadBuf(), delayMonitorLogEvent.getUpdateRowsEvent().getPayloadBuf());

        Assert.assertNotEquals(0, clone.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertNotEquals(0, clone.getPayloadBuf().refCnt());
        Assert.assertNotEquals(0, clone.getUpdateRowsEvent().getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertNotEquals(0, clone.getUpdateRowsEvent().getPayloadBuf().refCnt());
        Assert.assertNotEquals(0, delayMonitorLogEvent.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertNotEquals(0, delayMonitorLogEvent.getPayloadBuf().refCnt());
        Assert.assertNotEquals(0, delayMonitorLogEvent.getUpdateRowsEvent().getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertNotEquals(0, delayMonitorLogEvent.getUpdateRowsEvent().getPayloadBuf().refCnt());

        clone.release();
        if (delayMonitorLogEvent.isNeedReleased() && "fat".equalsIgnoreCase(delayMonitorLogEvent.getSrcDcName())) {
            delayMonitorLogEvent.release();
        }
        Assert.assertEquals(0, clone.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(0, clone.getPayloadBuf().refCnt());
        Assert.assertEquals(0, clone.getUpdateRowsEvent().getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(0, clone.getUpdateRowsEvent().getPayloadBuf().refCnt());
        Assert.assertEquals(0, delayMonitorLogEvent.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(0, delayMonitorLogEvent.getPayloadBuf().refCnt());
        Assert.assertEquals(0, delayMonitorLogEvent.getUpdateRowsEvent().getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(0, delayMonitorLogEvent.getUpdateRowsEvent().getPayloadBuf().refCnt());
    }

    /**
     * # at 13702
     * #200417 18:40:14 server id 100  end_log_pos 13800 CRC32 0x3f89e717
     * # Position  Timestamp   Type   Master ID        Size      Master Pos    Flags
     * #     18e2 7b 68 56 62   1f   65 00 00 00   66 00 00 00   db ec 13 16   00 00
     * #     18f5 6e 00 00 00 00 00 01 00  02 00 04 ff ff f0 fd 0c |n...............|
     * #     1905 00 00 00 00 00 00 05 73  68 61 72 62 0a 00 64 72 |.......sharb..dr|
     * #     1915 63 74 65 73 74 72 62 32  62 56 6a c1 08 b6 f0 fd |ctestrb2bVj.....|
     * #     1925 0c 00 00 00 00 00 00 05  73 68 61 72 62 0a 00 64 |........sharb..d|
     * #     1935 72 63 74 65 73 74 72 62  32 62 56 6a c2 09 10 2a |rctestrb2bVj....|
     * #     1945 82 6b a9
     *
     */
    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(98);
        byte[] bytes = new byte[] {
                (byte) 0x7b, (byte) 0x68, (byte) 0x56, (byte) 0x62, (byte) 0x1f, (byte) 0x65, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x66, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xdb, (byte) 0xec, (byte) 0x13, (byte) 0x16, (byte) 0x00, (byte) 0x00,
                (byte) 0x6e, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0xff, (byte) 0xff, (byte) 0xf0, (byte) 0xfd, (byte) 0x0c,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x62, (byte) 0x0a, (byte) 0x00, (byte) 0x64, (byte) 0x72,
                (byte) 0x63, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x72, (byte) 0x62, (byte) 0x32, (byte) 0x62, (byte) 0x56, (byte) 0x6a, (byte) 0xc1, (byte) 0x08, (byte) 0xb6, (byte) 0xf0, (byte) 0xfd,

                (byte) 0x0c, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x62, (byte) 0x0a, (byte) 0x00, (byte) 0x64,
                (byte) 0x72, (byte) 0x63, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x72, (byte) 0x62, (byte) 0x32, (byte) 0x62, (byte) 0x56, (byte) 0x6a, (byte) 0xc2, (byte) 0x09, (byte) 0x10, (byte) 0x2a,

                (byte) 0x82, (byte) 0x6b, (byte) 0xa9
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

}
