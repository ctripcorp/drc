package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.EMPTY_PREVIOUS_GTID_EVENT_SIZE;

/**
 * Created by @author zhuYongMing on 2019/9/14.
 */
public class PreviousGtidsLogEventTest {

    @Test
    public void readTest() {
        final ByteBuf byteBuf = initByteBuf();
        final PreviousGtidsLogEvent previousGtidsLogEvent = new PreviousGtidsLogEvent().read(byteBuf);
        if (null == previousGtidsLogEvent) {
            Assert.fail();
        }

        if (null == previousGtidsLogEvent.getLogEventHeader()) {
            Assert.fail();
        }

        final GtidSet gtidSet = previousGtidsLogEvent.getGtidSet();
        final GtidSet targetGtidSet = initGtidSet();

        Assert.assertEquals(targetGtidSet, gtidSet);
        Assert.assertEquals(0, previousGtidsLogEvent.getPayloadBuf().readableBytes());
    }

    @Test
    public void testEmptyPreviousGtidsLogEvent() throws IOException {
        PreviousGtidsLogEvent clone = new PreviousGtidsLogEvent(0, 0, new GtidSet(""));
        int size = clone.getLogEventHeader().getHeaderBuf().writerIndex() + clone.getPayloadBuf().writerIndex();
        Assert.assertEquals(size, EMPTY_PREVIOUS_GTID_EVENT_SIZE);
        clone.release();
    }

    @Test
    public void constructPreviousGtidsLogEventAndWriteTest() throws IOException, InterruptedException {
        final PreviousGtidsLogEvent constructPreviousGtidsLogEvent = new PreviousGtidsLogEvent(10000L, 123, initGtidSet());

        final ByteBuf writeByteBuf = ByteBufAllocator.DEFAULT.directBuffer();
        constructPreviousGtidsLogEvent.write(new IoCache() {
            @Override
            public void write(byte[] data) {
            }

            @Override
            public void write(Collection<ByteBuf> byteBufs) {
                for (ByteBuf byteBuf : byteBufs) {
                    final byte[] bytes = new byte[byteBuf.writerIndex()];
                    for (int i = 0; i < byteBuf.writerIndex(); i++) {
                        bytes[i] = byteBuf.getByte(i);
                    }
                    writeByteBuf.writeBytes(bytes);
                }
            }

            @Override
            public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
            }

            @Override
            public void write(LogEvent logEvent) {
            }
        });

        TimeUnit.SECONDS.sleep(1);
        final PreviousGtidsLogEvent readPreviousGtidsLogEvent = new PreviousGtidsLogEvent().read(writeByteBuf);
        Assert.assertEquals(readPreviousGtidsLogEvent.getGtidSet(), constructPreviousGtidsLogEvent.getGtidSet());
    }

    private GtidSet initGtidSet() {
        return new GtidSet(
                "5f9a1806-e024-11e9-8588-fa163e7af2aa:1-3,5f9a1806-e024-11e9-8588-fa163e7af2ac:1-3:5:7-8,5f9a1806-e024-11e9-8588-fa163e7af2ad:1-18"
        );
    }

    /**
     * binlog:
     * # at 123
     * #190927 13:51:57 server id 10000  end_log_pos 306 CRC32 0x52c3bfa5      Previous-GTIDs
     * # 5f9a1806-e024-11e9-8588-fa163e7af2aa:1-3,
     * # 5f9a1806-e024-11e9-8588-fa163e7af2ac:1-3:5:7-8,
     * # 5f9a1806-e024-11e9-8588-fa163e7af2ad:1-18
     * <p>
     * binary:
     * 0000007b  7d a3 8d 5d 23 10 27 00  00 b7 00 00 00 32 01 00  |}..]#.'......2..|
     * 0000008b  00 80 00 03 00 00 00 00  00 00 00 5f 9a 18 06 e0  |..........._....|
     * 0000009b  24 11 e9 85 88 fa 16 3e  7a f2 aa 01 00 00 00 00  |$......>z.......|
     * 000000ab  00 00 00 01 00 00 00 00  00 00 00 04 00 00 00 00  |................|
     * 000000bb  00 00 00 5f 9a 18 06 e0  24 11 e9 85 88 fa 16 3e  |..._....$......>|
     * 000000cb  7a f2 ac 03 00 00 00 00  00 00 00 01 00 00 00 00  |z...............|
     * 000000db  00 00 00 04 00 00 00 00  00 00 00 05 00 00 00 00  |................|
     * 000000eb  00 00 00 06 00 00 00 00  00 00 00 07 00 00 00 00  |................|
     * 000000fb  00 00 00 09 00 00 00 00  00 00 00 5f 9a 18 06 e0  |..........._....|
     * 0000010b  24 11 e9 85 88 fa 16 3e  7a f2 ad 01 00 00 00 00  |$......>z.......|
     * 0000011b  00 00 00 01 00 00 00 00  00 00 00 13 00 00 00 00  |................|
     * 0000012b  00 00 00 a5 bf c3 52                              |......R|
     */
    private ByteBuf initByteBuf() {
        String hexString =
                "7d a3 8d 5d 23 10 27 00  00 b7 00 00 00 32 01 00" +
                        "00 80 00 03 00 00 00 00  00 00 00 5f 9a 18 06 e0" +
                        "24 11 e9 85 88 fa 16 3e  7a f2 aa 01 00 00 00 00" +
                        "00 00 00 01 00 00 00 00  00 00 00 04 00 00 00 00" +
                        "00 00 00 5f 9a 18 06 e0  24 11 e9 85 88 fa 16 3e" +
                        "7a f2 ac 03 00 00 00 00  00 00 00 01 00 00 00 00" +
                        "00 00 00 04 00 00 00 00  00 00 00 05 00 00 00 00" +
                        "00 00 00 06 00 00 00 00  00 00 00 07 00 00 00 00" +
                        "00 00 00 09 00 00 00 00  00 00 00 5f 9a 18 06 e0" +
                        "24 11 e9 85 88 fa 16 3e  7a f2 ad 01 00 00 00 00" +
                        "00 00 00 01 00 00 00 00  00 00 00 13 00 00 00 00" +
                        "00 00 00 a5 bf c3 52";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
