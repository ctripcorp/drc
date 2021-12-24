package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by @author zhuYongMing on 2019/9/15.
 */
public class QueryLogEventTest {

    @Test
    public void readTest() {
        final ByteBuf byteBuf = initByteBuf();
        final QueryLogEvent queryLogEvent = new QueryLogEvent().read(byteBuf);
        if (null == queryLogEvent) {
            Assert.fail();
        }

        if (null == queryLogEvent.getLogEventHeader()) {
            Assert.fail();
        }

        Assert.assertEquals(199, queryLogEvent.getSlaveProxyId());
//        Assert.assertEquals(0, queryLogEvent.getExecuteTime()); why 0?
//        Assert.assertEquals(0, queryLogEvent.getSchemaLength()); why 0?
        Assert.assertEquals(0, queryLogEvent.getErrorCode());
        Assert.assertEquals(31, queryLogEvent.getStatusVarLength());

        final QueryLogEvent.QueryStatus queryStatus = queryLogEvent.getQueryStatus();
        Assert.assertEquals(0, queryStatus.getFlags2());
        Assert.assertEquals(1436549152, queryStatus.getSqlMode());
        Assert.assertEquals("std", queryStatus.getCatalog());
        Assert.assertEquals(2, queryStatus.getAutoIncrementIncrement());
        Assert.assertEquals(2, queryStatus.getAutoIncrementOffset());
        Assert.assertEquals(33, queryStatus.getClientCharset());
        Assert.assertEquals(33, queryStatus.getClientCollation());
        Assert.assertEquals(8, queryStatus.getServerCollation());
        Assert.assertNull(queryStatus.getTimeZone());
        Assert.assertEquals(-1, queryStatus.getLcTime());
        Assert.assertEquals(-1, queryStatus.getCharsetDatabase());
        Assert.assertEquals(-1, queryStatus.getTableMapForUpdate());
        Assert.assertEquals(-1, queryStatus.getMasterDataWritten());
        Assert.assertNull(queryStatus.getUser());
        Assert.assertNull(queryStatus.getHost());
        Assert.assertEquals(-1, queryStatus.getMicroseconds());
        Assert.assertNull(queryStatus.getUpdateDbNames());

//        Assert.assertEquals("", queryLogEvent.getSchemaName()); why?
        Assert.assertEquals("BEGIN", queryLogEvent.getQuery());
        Assert.assertEquals("4e446c07", Long.toHexString(queryLogEvent.getChecksum()));

        Assert.assertEquals(0 ,queryLogEvent.getPayloadBuf().readableBytes());
    }

    /**
     * | mysql-bin.000019 | 1443 | Query          |         1 |        1516 | BEGIN
     *
     * # at 1443
     * #190915 23:49:37 server id 1  end_log_pos 1516 CRC32 0x4e446c07         Query   thread_id=199   exec_time=0     error_code=0
     * SET TIMESTAMP=1568562577
     * BEGIN
     *
     * 000005a3  91 5d 7e 5d 02 01 00 00  00 49 00 00 00 ec 05 00  |.]~].....I......|
     * 000005b3  00 08 00 c7 00 00 00 00  00 00 00 00 00 00 1f 00  |................|
     * 000005c3  00 00 00 00 00 01 20 00  a0 55 00 00 00 00 06 03  |...... ..U......|
     * 000005d3  73 74 64 03 02 00 02 00  04 21 00 21 00 08 00 00  |std......!.!....|
     * 000005e3  42 45 47 49 4e 07 6c 44  4e
     */
    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(35);
        byte[] bytes = new byte[] {
                (byte) 0x91, (byte) 0x5d, (byte) 0x7e, (byte) 0x5d, (byte) 0x02, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x49, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xec, (byte) 0x05, (byte) 0x00,

                (byte) 0x00, (byte) 0x08, (byte) 0x00, (byte) 0xc7, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00,


                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x20, (byte) 0x00,
                (byte) 0xa0, (byte) 0x55, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x06, (byte) 0x03,

                (byte) 0x73, (byte) 0x74, (byte) 0x64, (byte) 0x03, (byte) 0x02, (byte) 0x00, (byte) 0x02, (byte) 0x00,
                (byte) 0x04, (byte) 0x21, (byte) 0x00, (byte) 0x21, (byte) 0x00, (byte) 0x08, (byte) 0x00, (byte) 0x00,


                (byte) 0x42, (byte) 0x45, (byte) 0x47, (byte) 0x49, (byte) 0x4e, (byte) 0x07, (byte) 0x6c, (byte) 0x44,
                (byte) 0x4e
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
