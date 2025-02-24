package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.format_description_log_event;

/**
 * Created by @author zhuYongMing on 2019/9/14.
 */
public class FormatDescriptionLogEventTest {

    @Test
    public void readTest() {
        final ByteBuf byteBuf = initByteBuf();
        final FormatDescriptionLogEvent formatDescriptionLogEvent =
                new FormatDescriptionLogEvent().read(byteBuf);

        if (null == formatDescriptionLogEvent) {
            Assert.fail();
        }

        if (null == formatDescriptionLogEvent.getLogEventHeader()) {
            Assert.fail();
        }

        // valid decode
        Assert.assertEquals(format_description_log_event, LogEventType.getLogEventType(formatDescriptionLogEvent.getLogEventHeader().getEventType()));
        Assert.assertEquals(4, formatDescriptionLogEvent.getBinlogVersion());
        Assert.assertEquals("5.7.27-log", formatDescriptionLogEvent.getMysqlServerVersion());
        Assert.assertEquals(0, formatDescriptionLogEvent.getBinlogCreateTimestamp());
        Assert.assertEquals(19, formatDescriptionLogEvent.getEventHeaderLength());
        Assert.assertEquals(38, formatDescriptionLogEvent.getEventPostHeaderLengths().length);
        Assert.assertEquals("c4b5a0bf", Long.toHexString(formatDescriptionLogEvent.getChecksum()));

        Assert.assertEquals(119, byteBuf.readerIndex());
        Assert.assertEquals(100, formatDescriptionLogEvent.getPayloadBuf().readerIndex());
        Assert.assertEquals(19, formatDescriptionLogEvent.getLogEventHeader().getHeaderBuf().readerIndex());
    }


    private static final int LOG_EVENT_BINLOG_IN_USE_F = 0x1;

    @Test
    public void testGetMySQLV8Format() {
        ByteBuf byteBuf = ByteHelper.getFormatDescriptionLogEventV2();
        FormatDescriptionLogEvent logEvent = new FormatDescriptionLogEvent().read(byteBuf);

        LogEventHeader logEventHeader = logEvent.getLogEventHeader();
        Assert.assertEquals(218, logEventHeader.getEventSize());
        Assert.assertEquals(222, logEventHeader.getNextEventStartPosition());

        Assert.assertEquals(0, logEventHeader.getFlags() & LOG_EVENT_BINLOG_IN_USE_F);
        Assert.assertEquals("8.0.36", logEvent.getMysqlServerVersion());

        // test eventPostHeaderLength
        int eventPostHeaderLength = logEvent.getEventPostHeaderLengths().length;
        Assert.assertEquals(137, eventPostHeaderLength);
        int maxType = Arrays.stream(LogEventType.values()).mapToInt(LogEventType::getType).max().getAsInt();
        Assert.assertTrue(eventPostHeaderLength > maxType);
    }


    /**
     * # at 4
     * #190914 20:56:13 server id 1  end_log_pos 123 CRC32 0xc4b5a0bf  Start: binlog v 4, server v 5.7.27-log created 190914 20:56:13
     * # Warning: this binlog is either in use or was not closed properly.
     *
     *             6d e3 7c 5d  0f 01 00 00 00 77 00 00
     * 00 7b 00 00 00 01 00 04  00 35 2e 37 2e 32 37 2d
     * 6c 6f 67 00 00 00 00 00  00 00 00 00 00 00 00 00
     * 00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00
     * 00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 13
     * 38 0d 00 08 00 12 00 04  04 04 04 12 00 00 5f 00
     * 04 1a 08 00 00 00 08 08  08 02 00 00 00 0a 0a 0a
     * 2a 2a 00 12 34 00 01 bf  a0 b5 c4
     */
    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(119);
        byte[] bytes = new byte[] {
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

                (byte) 0x38, (byte) 0x0d, (byte) 0x00, (byte) 0x08, (byte) 0x00, (byte) 0x12, (byte) 0x00, (byte) 0x04,  // 39 event count,
                (byte) 0x04, (byte) 0x04, (byte) 0x04, (byte) 0x12, (byte) 0x00, (byte) 0x00, (byte) 0x5f, (byte) 0x00,

                (byte) 0x04, (byte) 0x1a, (byte) 0x08, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x08, (byte) 0x08,
                (byte) 0x08, (byte) 0x02, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0a, (byte) 0x0a, (byte) 0x0a,

                (byte) 0x2a, (byte) 0x2a, (byte) 0x00, (byte) 0x12, (byte) 0x34, (byte) 0x00, (byte) 0x01, (byte) 0xbf,
                (byte) 0xa0, (byte) 0xb5, (byte) 0xc4
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

}
