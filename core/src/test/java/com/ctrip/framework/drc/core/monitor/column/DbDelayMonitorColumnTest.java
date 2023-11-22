package com.ctrip.framework.drc.core.monitor.column;

import com.ctrip.framework.drc.core.driver.binlog.impl.DelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.List;


/**
 * Created by jixinwang on 2021/11/11
 */
public class DbDelayMonitorColumnTest {
    byte[] dbDelayBytes = new byte[]{
            (byte) 0x51, (byte) 0x96, (byte) 0x5c, (byte) 0x65, (byte) 0x1f, (byte) 0xea, (byte) 0x0c, (byte) 0x00, (byte) 0x00, (byte) 0xd0, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xd7, (byte) 0xf1, (byte) 0xd0, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
            (byte) 0x30, (byte) 0x5D, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x03, (byte) 0xFF, (byte) 0xFF, (byte) 0x00, (byte) 0xD9, (byte) 0x41,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x45, (byte) 0x00, (byte) 0x7B, (byte) 0x22, (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74,
            (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22,
            (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31,
            (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x62, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F,
            (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x64, (byte) 0x5F, (byte) 0x64, (byte) 0x62, (byte) 0x31, (byte) 0x36, (byte) 0x32, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x96,
            (byte) 0x4F, (byte) 0x1C, (byte) 0x98, (byte) 0x00, (byte) 0xD9, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x45, (byte) 0x00, (byte) 0x7B, (byte) 0x22,
            (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22,
            (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F,
            (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x62, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E,
            (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x64, (byte) 0x5F, (byte) 0x64, (byte) 0x62, (byte) 0x31, (byte) 0x36,
            (byte) 0x32, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x96, (byte) 0x51, (byte) 0x0E, (byte) 0xB0, (byte) 0xD4, (byte) 0xAC, (byte) 0x61, (byte) 0xAB,
    };

    byte[] mhaDelayBytes = new byte[]{
            (byte) 0x01, (byte) 0x97, (byte) 0x5c, (byte) 0x65, (byte) 0x1f, (byte) 0xea, (byte) 0x0c, (byte) 0x00, (byte) 0x00, (byte) 0xa6, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x2a, (byte) 0x59, (byte) 0x2c, (byte) 0x22, (byte) 0x00, (byte) 0x00,
            (byte) 0x59, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0xFF, (byte) 0xFF, (byte) 0x00, (byte) 0x6E, (byte) 0x0D,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x2A, (byte) 0x00, (byte) 0x7B, (byte) 0x22,
            (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22,
            (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F,
            (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x97, (byte) 0x00, (byte) 0x05, (byte) 0x8C, (byte) 0x00, (byte) 0x6E,
            (byte) 0x0D, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x2A, (byte) 0x00, (byte) 0x7B,
            (byte) 0x22, (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A,
            (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E,
            (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x97, (byte) 0x01, (byte) 0x12, (byte) 0x3E, (byte) 0xD3,
            (byte) 0x10, (byte) 0x25, (byte) 0x99,
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
        Assert.assertEquals("ntgxh", delayMonitorSrcDcName);
        List<List<Object>> afterPresentRowsValues = DelayMonitorColumn.getAfterPresentRowsValues(delayMonitorLogEvent);
        Assert.assertEquals(4, afterPresentRowsValues.get(0).size());
        delayMonitorLogEvent.release();
    }

    @Test
    public void testMatchThenParse() {
        ByteBuf eventByteBuf = initByteBuf(mhaDelayBytes);
        DelayMonitorLogEvent delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        Assert.assertFalse(DbDelayMonitorColumn.match(delayMonitorLogEvent));
        Assert.assertTrue(DelayMonitorColumn.match(delayMonitorLogEvent));
        String delayMonitorSrcDcName = DelayMonitorColumn.getDelayMonitorSrcDcName(delayMonitorLogEvent);
        Assert.assertEquals("ntgxh", delayMonitorSrcDcName);

        eventByteBuf = initByteBuf(dbDelayBytes);
        delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        Assert.assertTrue(DbDelayMonitorColumn.match(delayMonitorLogEvent));
        Assert.assertFalse(DelayMonitorColumn.match(delayMonitorLogEvent));
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
        Assert.assertEquals(3, afterPresentRowsValues2.get(0).size());

        // 4. get dto
        DbDelayDto dbDelayDto = DbDelayMonitorColumn.parseEvent(delayMonitorLogEvent);
        Assert.assertNotNull(dbDelayDto);
        System.out.println(dbDelayDto);
        Assert.assertEquals("zyn_test_1", dbDelayDto.getMha());
        Assert.assertEquals("zyn_test_shard_db162", dbDelayDto.getDbName());
        Assert.assertEquals("ntgxh", dbDelayDto.getRegion());
        Assert.assertEquals("ntgxh", dbDelayDto.getDcName());
        Assert.assertEquals(Long.valueOf(1700566609376L), dbDelayDto.getDatachangeLasttime());
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

    @Test
    public void testPrint() {
        ByteBuf eventByteBuf = initByteBuf(mhaDelayBytes);
        ByteBuf dbeventByteBuf = initByteBuf(dbDelayBytes);
        DelayMonitorLogEvent delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        DelayMonitorLogEvent dbDelayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(dbeventByteBuf));
        System.out.println(DbDelayMonitorColumn.getUpdateRowsBytes(delayMonitorLogEvent.getUpdateRowsEvent()));
        System.out.println(DbDelayMonitorColumn.getUpdateRowsBytes(dbDelayMonitorLogEvent.getUpdateRowsEvent()));
    }
}
