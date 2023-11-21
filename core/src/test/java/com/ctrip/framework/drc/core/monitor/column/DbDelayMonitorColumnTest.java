package com.ctrip.framework.drc.core.monitor.column;

import com.ctrip.framework.drc.core.driver.binlog.impl.DelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by jixinwang on 2021/11/11
 */
public class DbDelayMonitorColumnTest {
    private static byte[] mhaDelayBytes = new byte[]{
            (byte) 0xec, (byte) 0x95, (byte) 0x55, (byte) 0x65, (byte) 0x1f, (byte) 0x64, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa1, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9b, (byte) 0x9d, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x82, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0xFF, (byte) 0xFF, (byte) 0x00, (byte) 0x02, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x62, (byte) 0x27, (byte) 0x00, (byte) 0x7B, (byte) 0x22,
            (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x62, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22,
            (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x70, (byte) 0x68, (byte) 0x64, (byte) 0x5F, (byte) 0x74, (byte) 0x65,
            (byte) 0x73, (byte) 0x74, (byte) 0x32, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x55, (byte) 0x95, (byte) 0x91, (byte) 0x12, (byte) 0x8E, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x00, (byte) 0x00,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x28, (byte) 0x00, (byte) 0x7B, (byte) 0x22, (byte) 0x64, (byte) 0x22,
            (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x73, (byte) 0x68,
            (byte) 0x61, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74,
            (byte) 0x5F, (byte) 0x31, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x55, (byte) 0x95, (byte) 0xEC, (byte) 0x14, (byte) 0xD2, (byte) 0xD6, (byte) 0xA3, (byte) 0xB7, (byte) 0xCE
    };

    private static byte[] dbDelayBytes = new byte[]{
            (byte) 0x65, (byte) 0x23, (byte) 0x57, (byte) 0x65, (byte) 0x1f, (byte) 0xea, (byte) 0x0c, (byte) 0x00, (byte) 0x00, (byte) 0x8e, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xf7, (byte) 0x1f, (byte) 0xc1, (byte) 0x2f, (byte) 0x00, (byte) 0x00,
            (byte) 0x9A, (byte) 0x91, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x06, (byte) 0xFF, (byte) 0xFF, (byte) 0x00, (byte) 0x5B, (byte) 0x33,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0A, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F,
            (byte) 0x31, (byte) 0x0E, (byte) 0x67, (byte) 0x68, (byte) 0x6F, (byte) 0x73, (byte) 0x74, (byte) 0x31, (byte) 0x5F, (byte) 0x75, (byte) 0x6E, (byte) 0x69, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74,
            (byte) 0x05, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x05, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x65, (byte) 0x57, (byte) 0x23, (byte) 0x64,
            (byte) 0x17, (byte) 0x48, (byte) 0x00, (byte) 0x5B, (byte) 0x33, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0A, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F,
            (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31, (byte) 0x0E, (byte) 0x67, (byte) 0x68, (byte) 0x6F, (byte) 0x73, (byte) 0x74, (byte) 0x31, (byte) 0x5F, (byte) 0x75, (byte) 0x6E,
            (byte) 0x69, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x05, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x05, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78,
            (byte) 0x68, (byte) 0x65, (byte) 0x57, (byte) 0x23, (byte) 0x65, (byte) 0x18, (byte) 0xD8, (byte) 0x78, (byte) 0xE5, (byte) 0xAB, (byte) 0xFF,
    };
    private static final String gtid = "abcde123-5678-1234-abcd-abcd1234abcd:123456789";

    private static final String idc = "ntgxh";

    private ByteBuf initByteBuf(byte[] bytes) {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(98);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    @Test
    public void testMatch() {
        ByteBuf eventByteBuf = initByteBuf(dbDelayBytes);
        DelayMonitorLogEvent delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        Assert.assertTrue(DbDelayMonitorColumn.match(delayMonitorLogEvent));

    }

    @Test
    public void testReadOrigin() {
        ByteBuf eventByteBuf = initByteBuf(mhaDelayBytes);
        DelayMonitorLogEvent delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        String delayMonitorSrcDcName = DelayMonitorColumn.getDelayMonitorSrcDcName(delayMonitorLogEvent);
        Assert.assertEquals("sha", delayMonitorSrcDcName);
        List<List<Object>> afterPresentRowsValues = DelayMonitorColumn.getAfterPresentRowsValues(delayMonitorLogEvent);
        Assert.assertEquals(4, afterPresentRowsValues.get(0).size());
        delayMonitorLogEvent.release();
    }

    @Test
    public void testMatchThenParse() {
        ByteBuf eventByteBuf = initByteBuf(mhaDelayBytes);
        DelayMonitorLogEvent delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        Assert.assertFalse(DbDelayMonitorColumn.match(delayMonitorLogEvent));
        String delayMonitorSrcDcName = DelayMonitorColumn.getDelayMonitorSrcDcName(delayMonitorLogEvent);
        Assert.assertEquals("sha", delayMonitorSrcDcName);

        eventByteBuf = initByteBuf(dbDelayBytes);
        delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        Assert.assertTrue(DbDelayMonitorColumn.match(delayMonitorLogEvent));
        delayMonitorSrcDcName = DbDelayMonitorColumn.getDelayMonitorSrcDcName(delayMonitorLogEvent);
        Assert.assertEquals("ntgxh", delayMonitorSrcDcName);
    }

    @Test
    public void testParseDbDelayEvent() throws ParseException {
        ByteBuf dbEventByteBuf = initByteBuf(dbDelayBytes);
        DelayMonitorLogEvent delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(dbEventByteBuf));

        // 1. match
        Assert.assertTrue(DbDelayMonitorColumn.match(delayMonitorLogEvent));

        // 2. get src name
        String delayMonitorSrcDcName = DbDelayMonitorColumn.getDelayMonitorSrcDcName(delayMonitorLogEvent);
        Assert.assertEquals(delayMonitorSrcDcName, "ntgxh");

        // 3. get rows
        List<List<Object>> afterPresentRowsValues2 = DbDelayMonitorColumn.getAfterPresentRowsValues(delayMonitorLogEvent);
        Assert.assertEquals(6, afterPresentRowsValues2.get(0).size());

        // 4. get dto
        DbDelayDto dbDelayDto = DbDelayMonitorColumn.parseEvent(delayMonitorLogEvent);
        Assert.assertNotNull(dbDelayDto);
        System.out.println(dbDelayDto);
        Assert.assertEquals("zyn_test_1", dbDelayDto.getMha());
        Assert.assertEquals("ghost1_unitest", dbDelayDto.getDbName());
        Assert.assertEquals("ntgxh", dbDelayDto.getRegion());
        Assert.assertEquals("ntgxh", dbDelayDto.getDcName());
        Assert.assertEquals(Long.valueOf(1700209509636L), dbDelayDto.getDatachangeLasttime());
        delayMonitorLogEvent.release();
    }


    @Test
    public void testLoadTwice() {
        ByteBuf eventByteBuf = initByteBuf(mhaDelayBytes);
        DelayMonitorLogEvent delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));

        // 1. test load normal twice
        List<List<Object>> rows1 = DelayMonitorColumn.getAfterPresentRowsValues(delayMonitorLogEvent);
        List<List<Object>> rwos2 = DelayMonitorColumn.getAfterPresentRowsValues(delayMonitorLogEvent);
        Assert.assertEquals(rows1, rwos2);

        delayMonitorLogEvent.release();
    }
}
