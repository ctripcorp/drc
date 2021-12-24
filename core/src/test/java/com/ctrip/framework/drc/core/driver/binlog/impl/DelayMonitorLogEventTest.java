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
     * #     3586 8e 87 99 5e   1f   64 00 00 00   62 00 00 00   e8 35 00 00   00 00
     * #     3599 7b 00 00 00 00 00 01 00  02 00 04 ff ff f0 02 00 |................|
     * #     35a9 00 00 00 00 00 00 07 74  65 73 74 5f 6f 79 07 74 |.......test.oy.t|
     * #     35b9 65 73 74 5f 6f 79 5e 99  87 77 1c fc f0 02 00 00 |est.oy...w......|
     * #     35c9 00 00 00 00 00 07 74 65  73 74 5f 6f 79 07 74 65 |......test.oy.te|
     * #     35d9 73 74 5f 6f 79 5e 99 87  8e 10 cc 17 e7 89 3f    |st.oy..........|
     *
     */
    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(98);
        byte[] bytes = new byte[] {
                (byte) 0x8e, (byte) 0x87, (byte) 0x99, (byte) 0x5e, (byte) 0x1f, (byte) 0x64, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x62, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xe8, (byte) 0x35, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0xff, (byte) 0xff, (byte) 0xf0, (byte) 0x02, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5f, (byte) 0x6f, (byte) 0x79, (byte) 0x07, (byte) 0x74,
                (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5f, (byte) 0x6f, (byte) 0x79, (byte) 0x5e, (byte) 0x99, (byte) 0x87, (byte) 0x77, (byte) 0x1c, (byte) 0xfc, (byte) 0xf0, (byte) 0x02, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5f, (byte) 0x6f, (byte) 0x79, (byte) 0x07, (byte) 0x74, (byte) 0x65,
                (byte) 0x73, (byte) 0x74, (byte) 0x5f, (byte) 0x6f, (byte) 0x79, (byte) 0x5e, (byte) 0x99, (byte) 0x87, (byte) 0x8e, (byte) 0x10, (byte) 0xcc, (byte) 0x17, (byte) 0xe7, (byte) 0x89, (byte) 0x3f
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
