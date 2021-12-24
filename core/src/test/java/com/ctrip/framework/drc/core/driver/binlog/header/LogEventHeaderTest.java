package com.ctrip.framework.drc.core.driver.binlog.header;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.update_rows_event_v2;

/**
 * Created by @author zhuYongMing on 2019/9/12.
 */
public class LogEventHeaderTest {

    @Test
    public void readTest() {
        final ByteBuf byteBuf = initByteBuf();
        final LogEventHeader header = new LogEventHeader().read(byteBuf);
        if (null == header) {
            Assert.fail();
        }

        // decode header
        Assert.assertEquals(1568172926, header.getEventTimestamp());
        Assert.assertEquals(31, header.getEventType());
        Assert.assertEquals(1, header.getServerId());
        Assert.assertEquals(64, header.getEventSize());
        Assert.assertEquals(12246, header.getNextEventStartPosition());
        Assert.assertEquals(0, header.getFlags());

        Assert.assertEquals(eventHeaderLengthVersionGt1, byteBuf.readerIndex());
        Assert.assertEquals(eventHeaderLengthVersionGt1, header.getHeaderBuf().readerIndex());
        Assert.assertEquals(eventHeaderLengthVersionGt1, header.getHeaderBuf().writerIndex());
    }

    @Test
    public void constructLogEventHeaderAndWriteTest() {
        final LogEventHeader header = new LogEventHeader(
                update_rows_event_v2.getType(), 1L, 64, 12246
        );

        Assert.assertEquals(0, header.getHeaderBuf().readableBytes());
        Assert.assertEquals(31, header.getEventType());
        Assert.assertEquals(1, header.getServerId());
        Assert.assertEquals(64, header.getEventSize());
        Assert.assertEquals(12246, header.getNextEventStartPosition());
        Assert.assertEquals(0, header.getFlags());

        header.write(new IoCache() {
            @Override
            public void write(byte[] data) {

            }

            @Override
            public void write(Collection<ByteBuf> byteBufs) {
                for (ByteBuf byteBuf : byteBufs) {
                    Assert.assertEquals(eventHeaderLengthVersionGt1, byteBuf.readerIndex());
                    Assert.assertEquals(eventHeaderLengthVersionGt1, byteBuf.writerIndex());

                    final byte[] initBytes = initBytes();
                    for (int i = 0; i < byteBuf.writerIndex(); i++) {
                        // timestamp, flags ignore
                        List<Integer> ignoreIndex = Lists.newArrayList(0, 1, 2, 3, 17, 18);
                        if (!ignoreIndex.contains(i)) {
                            final byte expectedByte = initBytes[i];
                            final byte actualByte = (byte) byteBuf.getUnsignedByte(i);
                            Assert.assertEquals(expectedByte, actualByte);
                        }
                    }
                }
            }

            @Override
            public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
            }

            @Override
            public void write(LogEvent logEvent) {
            }
        });


    }

    // 7e 6b 78 5d 1f 01 00 00 00 40 00 00 00 d6 2f 00
    // 00 00 00
    private byte[] initBytes() {
        return new byte[]{
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x1f, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x40, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xd6, (byte) 0x2f, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00
        };
    }

    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(eventHeaderLengthVersionGt1);

        byteBuf.writeBytes(initBytes());

        return byteBuf;
    }
}
