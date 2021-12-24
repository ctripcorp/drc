package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;

/**
 * Created by @author zhuYongMing on 2019/9/12.
 */
public class AbstractLogEventTest {

    @Test
    public void readFromMultiByteBufAndWriteTest () {
        final ByteBuf firstByteBuf = initNotEnoughEventFirstByteBuf();
        final ByteBuf secondByteBuf = initNotEnoughEventSecondByteBuf();
        final ByteBuf thirdByteBuf = initNotEnoughEventThirdByteBuf();
        final GtidLogEvent gtidLogEvent = new GtidLogEvent();

        // read
        final GtidLogEvent readFirst= gtidLogEvent.read(firstByteBuf);
        Assert.assertNull(readFirst);
        Assert.assertEquals(firstByteBuf.writerIndex(), firstByteBuf.readerIndex());
        Assert.assertEquals(eventHeaderLengthVersionGt1, gtidLogEvent.getLogEventHeader().getHeaderBuf().readerIndex());
        Assert.assertEquals(firstByteBuf.readerIndex() - eventHeaderLengthVersionGt1, gtidLogEvent.getPayloadBuf().writerIndex());

        final GtidLogEvent readSecond = gtidLogEvent.read(secondByteBuf);
        Assert.assertNull(readSecond);
        Assert.assertEquals(secondByteBuf.writerIndex(), secondByteBuf.readerIndex());
        Assert.assertEquals(
                firstByteBuf.readerIndex() - eventHeaderLengthVersionGt1 + secondByteBuf.readerIndex(),
                gtidLogEvent.getPayloadBuf().writerIndex()
        );

        final GtidLogEvent readThird = gtidLogEvent.read(thirdByteBuf);
        Assert.assertNotNull(readThird);
        Assert.assertEquals(1, thirdByteBuf.writerIndex() - thirdByteBuf.readerIndex()); // the third bytebuf has remaining
        Assert.assertEquals(
                firstByteBuf.readerIndex() - eventHeaderLengthVersionGt1 + secondByteBuf.readerIndex() + thirdByteBuf.readerIndex(),
                gtidLogEvent.getPayloadBuf().writerIndex()
        );

        final ByteBuf payloadBuf = gtidLogEvent.getPayloadBuf();
        Assert.assertEquals(payloadBuf.writerIndex(), payloadBuf.readerIndex());
        Assert.assertEquals(1, gtidLogEvent.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(1, gtidLogEvent.getPayloadBuf().refCnt());

        // write
        final AtomicInteger writeCount = new AtomicInteger(0);
        gtidLogEvent.write(new IoCache() {
            @Override
            public void write(byte[] data) {

            }

            @Override
            public void write(Collection<ByteBuf> byteBufs) {
                writeCount.incrementAndGet();
            }

            @Override
            public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
            }

            @Override
            public void write(LogEvent logEvent) {

            }
        });
        Assert.assertEquals(1, writeCount.get());

        // release
        try {
            gtidLogEvent.release();
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertEquals(0, gtidLogEvent.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(0, gtidLogEvent.getPayloadBuf().refCnt());
    }

    @Test(expected = IllegalStateException.class)
    public void writeBeforeFinishedReading() {
        final GtidLogEvent gtidLogEvent = new GtidLogEvent();
        gtidLogEvent.write(new IoCache() {
            @Override
            public void write(byte[] data) {

            }

            @Override
            public void write(Collection<ByteBuf>  byteBufs) {

            }

            @Override
            public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
            }

            @Override
            public void write(LogEvent logEvent) {

            }
        });
    }

    @Test
    public void readFromOneByteBufAndWrite() {
        final ByteBuf byteBuf = initByteBuf();
        final GtidLogEvent gtidLogEvent = new GtidLogEvent();

        // read
        Assert.assertNotNull(gtidLogEvent.read(byteBuf));
        Assert.assertEquals(1, byteBuf.readableBytes());
        Assert.assertEquals(1, gtidLogEvent.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(1, gtidLogEvent.getPayloadBuf().refCnt());

        // write
        final AtomicInteger writeCount = new AtomicInteger(0);
        gtidLogEvent.write(new IoCache() {
            @Override
            public void write(byte[] data) {

            }

            @Override
            public void write(Collection<ByteBuf>  byteBufs) {
                writeCount.incrementAndGet();
            }

            @Override
            public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
            }

            @Override
            public void write(LogEvent logEvent) {

            }
        });
        Assert.assertEquals(1, writeCount.get());

        // release
        try {
            gtidLogEvent.release();
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertEquals(0, gtidLogEvent.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(0, gtidLogEvent.getPayloadBuf().refCnt());
    }


    @Test
    public void isEmptyTest() {
        final ByteBuf byteBuf = initByteBuf();
        final GtidLogEvent gtidLogEvent = new GtidLogEvent();
        Assert.assertTrue(gtidLogEvent.isEmpty());

        gtidLogEvent.read(byteBuf);
        Assert.assertTrue(!gtidLogEvent.isEmpty());
    }

    private ByteBuf initNotEnoughEventFirstByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(32);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x2e, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0xa1, (byte) 0xfb, (byte) 0xb8,
                (byte) 0xbd, (byte) 0xc8, (byte) 0x11, (byte) 0xe9, (byte) 0x96, (byte) 0xa0, (byte) 0xfa, (byte) 0x16
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    private ByteBuf initNotEnoughEventSecondByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(16);
        byte[] bytes = new byte[] {
                (byte) 0x3e, (byte) 0x7a, (byte) 0xf2, (byte) 0xad, (byte) 0x42, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x1e, (byte) 0x00, (byte) 0x00,
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    private ByteBuf initNotEnoughEventThirdByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(18);
        byte[] bytes = new byte[] {
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40, (byte) 0xe9, (byte) 0xd9,

                (byte) 0x34,
                // this byte 0x00 is extra for test
                (byte) 0x00
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(66);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x2e, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0xa1, (byte) 0xfb, (byte) 0xb8,
                (byte) 0xbd, (byte) 0xc8, (byte) 0x11, (byte) 0xe9, (byte) 0x96, (byte) 0xa0, (byte) 0xfa, (byte) 0x16,

                (byte) 0x3e, (byte) 0x7a, (byte) 0xf2, (byte) 0xad, (byte) 0x42, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x1e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40, (byte) 0xe9, (byte) 0xd9,

                (byte) 0x34,
                // this byte 0x00 is extra for test
                (byte) 0x00
        };

        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
