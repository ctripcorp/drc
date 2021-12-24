package com.ctrip.framework.drc.replicator.impl.inbound.converter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by @author zhuYongMing on 2019/9/29.
 */
public class ReplicatorByteBufConverterTest {

    @Test
    public void testConvert() {
        final ReplicatorByteBufConverter converter = new ReplicatorByteBufConverter();

        final ByteBuf threeLogEventAndHalfWriteRowsLogEventByteBuf = initThreeLogEventAndHalfWriteRowsLogEventByteBuf();
        final List<LogEvent> logEvents1 = converter.convert(threeLogEventAndHalfWriteRowsLogEventByteBuf);
        Assert.assertEquals(0, threeLogEventAndHalfWriteRowsLogEventByteBuf.readableBytes());
        Assert.assertTrue(converter.hasUnFinishedLogEvent());
        Assert.assertNotNull(logEvents1);
        Assert.assertEquals(3, logEvents1.size());
        threeLogEventAndHalfWriteRowsLogEventByteBuf.release();

        final ByteBuf otherHalfWriteRowsLogEventAndHalfXidLogEvent = initOtherHalfWriteRowsLogEventAndHalfXidLogEvent();
        final List<LogEvent> logEvents2 = converter.convert(otherHalfWriteRowsLogEventAndHalfXidLogEvent);
        Assert.assertEquals(0, otherHalfWriteRowsLogEventAndHalfXidLogEvent.readableBytes());
        Assert.assertTrue(converter.hasUnFinishedLogEvent());
        Assert.assertNotNull(logEvents2);
        Assert.assertEquals(1, logEvents2.size());
        otherHalfWriteRowsLogEventAndHalfXidLogEvent.release();

        final ByteBuf oneQuarterXidLogEvent = initOneQuarterXidLogEvent();
        final List<LogEvent> logEvents3 = converter.convert(oneQuarterXidLogEvent);
        Assert.assertEquals(0, oneQuarterXidLogEvent.readableBytes());
        Assert.assertTrue(converter.hasUnFinishedLogEvent());
        Assert.assertNotNull(logEvents3);
        Assert.assertEquals(0, logEvents3.size());
        oneQuarterXidLogEvent.release();

        final ByteBuf remainingOneQuarterXidLogEvent = initRemainingOneQuarterXidLogEvent();
        final List<LogEvent> logEvents4 = converter.convert(remainingOneQuarterXidLogEvent);
        Assert.assertEquals(0, remainingOneQuarterXidLogEvent.readableBytes());
        Assert.assertTrue(!converter.hasUnFinishedLogEvent());
        Assert.assertNotNull(logEvents4);
        Assert.assertEquals(1, logEvents4.size());
        remainingOneQuarterXidLogEvent.release();
    }

    // 3 logEvent + 0.5 writeRowsLogEvent
    private ByteBuf initThreeLogEventAndHalfWriteRowsLogEventByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(1000);

        // formatDescription event
        byte[] formatDescriptionLogEventBytes = new byte[]{
                (byte) 0x6d, (byte) 0xe3, (byte) 0x7c, (byte) 0x5d,
                (byte) 0x0f, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x77, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x04,
                (byte) 0x00, (byte) 0x35, (byte) 0x2e, (byte) 0x37, (byte) 0x2e, (byte) 0x32, (byte) 0x37, (byte) 0x2d,

                (byte) 0x6c, (byte) 0x6f, (byte) 0x67, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x13,

                (byte) 0x38, (byte) 0x0d, (byte) 0x00, (byte) 0x08, (byte) 0x00, (byte) 0x12, (byte) 0x00, (byte) 0x04,
                (byte) 0x04, (byte) 0x04, (byte) 0x04, (byte) 0x12, (byte) 0x00, (byte) 0x00, (byte) 0x5f, (byte) 0x00,

                (byte) 0x04, (byte) 0x1a, (byte) 0x08, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x08, (byte) 0x08,
                (byte) 0x08, (byte) 0x02, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0a, (byte) 0x0a, (byte) 0x0a,

                (byte) 0x2a, (byte) 0x2a, (byte) 0x00, (byte) 0x12, (byte) 0x34, (byte) 0x00, (byte) 0x01, (byte) 0xbf,
                (byte) 0xa0, (byte) 0xb5, (byte) 0xc4
        };

        // gtid event
        byte[] gtidLogLogEventBytes = new byte[]{
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x2e, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0xa1, (byte) 0xfb, (byte) 0xb8,
                (byte) 0xbd, (byte) 0xc8, (byte) 0x11, (byte) 0xe9, (byte) 0x96, (byte) 0xa0, (byte) 0xfa, (byte) 0x16,

                (byte) 0x3e, (byte) 0x7a, (byte) 0xf2, (byte) 0xad, (byte) 0x42, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x1e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40, (byte) 0xe9, (byte) 0xd9,

                (byte) 0x34
        };

        // tableMap event
        byte[] tableMapLogEventBytes = new byte[] {
                (byte) 0x70, (byte) 0xe3, (byte) 0x7c, (byte) 0x5d, (byte) 0x13, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x6e, (byte) 0x03, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x09, (byte) 0x67, (byte) 0x74, (byte) 0x69, (byte) 0x64,

                (byte) 0x5f, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x00, (byte) 0x0a, (byte) 0x72,
                (byte) 0x6f, (byte) 0x77, (byte) 0x5f, (byte) 0x69, (byte) 0x6d, (byte) 0x61, (byte) 0x67, (byte) 0x65,

                (byte) 0x33, (byte) 0x00, (byte) 0x04, (byte) 0x03, (byte) 0xfe, (byte) 0x03, (byte) 0x0f, (byte) 0x04,
                (byte) 0xfe, (byte) 0x1e, (byte) 0x02, (byte) 0x01, (byte) 0x0f, (byte) 0x8e, (byte) 0x50, (byte) 0x7a,

                (byte) 0x4d
        };

        // half writeRowsLogEvent
        byte[] halfWriteRowsLogEventBytes = new byte[] {
                (byte) 0x91, (byte) 0x5d, (byte) 0x7e, (byte) 0x5d, (byte) 0x1e, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x31, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xbd, (byte) 0x06, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0x09, (byte) 0xfc
        };

        byteBuf.writeBytes(formatDescriptionLogEventBytes);
        byteBuf.writeBytes(gtidLogLogEventBytes);
        byteBuf.writeBytes(tableMapLogEventBytes);
        byteBuf.writeBytes(halfWriteRowsLogEventBytes);

        return byteBuf;
    }


    // 0.5 writeRowsLogEvent + 0.5 xidLogEvent
    private ByteBuf initOtherHalfWriteRowsLogEventAndHalfXidLogEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(100);
        byte[] otherHalfWriteRowsLogEventBytes = new byte[] {
                (byte) 0x09, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x00, (byte) 0x76, (byte) 0x61,
                (byte) 0x72, (byte) 0x63, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0xb8, (byte) 0x74, (byte) 0xe8,

                (byte) 0x80
        };

        byte[] halfXidLogEventBytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x10, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xf5, (byte) 0x2f, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x44, (byte) 0x04, (byte) 0x00, (byte) 0x00, (byte) 0x00
        };
        byteBuf.writeBytes(otherHalfWriteRowsLogEventBytes);
        byteBuf.writeBytes(halfXidLogEventBytes);

        return byteBuf;
    }

    // 0.25 xidLogEvent
    private ByteBuf initOneQuarterXidLogEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(100);
        byte[] oneQuarterXidLogEventBytes = new byte[] {
                (byte) 0x00, (byte) 0x00, (byte) 0x00
        };
        byteBuf.writeBytes(oneQuarterXidLogEventBytes);

        return byteBuf;
    }

    // remaining 0.25 xidLogEvent
    private ByteBuf initRemainingOneQuarterXidLogEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(100);
        byte[] remainingOneQuarterXidLogEventBytes = new byte[] {
                (byte) 0x82, (byte) 0xe5, (byte) 0xc7, (byte) 0x3a
        };
        byteBuf.writeBytes(remainingOneQuarterXidLogEventBytes);

        return byteBuf;
    }
}
