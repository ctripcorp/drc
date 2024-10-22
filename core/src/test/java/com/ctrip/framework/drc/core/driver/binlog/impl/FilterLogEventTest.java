package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.util.LogEventMockUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jixinwang on 2023/11/21
 */
public class FilterLogEventTest {

    @Test
    public void encode() {
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", FilterLogEvent.UNKNOWN, 0, 0, 101);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals(101, filterLogEvent.getNextTransactionOffset());

        ByteBuf headerBuf = filterLogEvent.getLogEventHeader().getHeaderBuf();
        headerBuf.readerIndex(0);
        ByteBuf payloadBuf = filterLogEvent.getPayloadBuf();
        payloadBuf.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, headerBuf, payloadBuf);

        FilterLogEvent newFilterLogEvent = new FilterLogEvent();
        newFilterLogEvent.read(compositeByteBuf);
        compositeByteBuf.release(compositeByteBuf.refCnt());
        Assert.assertEquals("drc1", newFilterLogEvent.getSchemaName());
        Assert.assertEquals(101, newFilterLogEvent.getNextTransactionOffset());
        Assert.assertEquals("unknown", newFilterLogEvent.getLastTableName());
        Assert.assertEquals(0, newFilterLogEvent.getEventCount());
    }

    @Test
    public void encodeV2() {
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        int count = 20000000;
        filterLogEvent.encode("drc1", "table1", count, count / 2, 101);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals("table1", filterLogEvent.getLastTableName());
        Assert.assertEquals(count, filterLogEvent.getEventCount());
        Assert.assertEquals(101, filterLogEvent.getNextTransactionOffset());

        ByteBuf headerBuf = filterLogEvent.getLogEventHeader().getHeaderBuf();
        headerBuf.readerIndex(0);
        ByteBuf payloadBuf = filterLogEvent.getPayloadBuf();
        payloadBuf.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, headerBuf, payloadBuf);

        FilterLogEvent newFilterLogEvent = new FilterLogEvent();
        newFilterLogEvent.read(compositeByteBuf);
        compositeByteBuf.release(compositeByteBuf.refCnt());
        Assert.assertEquals("drc1", newFilterLogEvent.getSchemaName());
        Assert.assertEquals(101, newFilterLogEvent.getNextTransactionOffset());
        Assert.assertEquals("table1", newFilterLogEvent.getLastTableName());
        Assert.assertEquals(false, newFilterLogEvent.isNoRowsEvent());
        Assert.assertEquals(count, newFilterLogEvent.getEventCount());
    }

    @Test
    public void encodeV3() {
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        int count = 20000000;
        filterLogEvent.encode("drc1", "table1", count, null, 101);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals("table1", filterLogEvent.getLastTableName());
        Assert.assertEquals(count, filterLogEvent.getEventCount());
        Assert.assertEquals(101, filterLogEvent.getNextTransactionOffset());
        Assert.assertEquals(false, filterLogEvent.isNoRowsEvent());
        Assert.assertEquals(null, filterLogEvent.getRowsEventCount());

        ByteBuf headerBuf = filterLogEvent.getLogEventHeader().getHeaderBuf();
        headerBuf.readerIndex(0);
        ByteBuf payloadBuf = filterLogEvent.getPayloadBuf();
        payloadBuf.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, headerBuf, payloadBuf);

        FilterLogEvent newFilterLogEvent = new FilterLogEvent();
        newFilterLogEvent.read(compositeByteBuf);
        compositeByteBuf.release(compositeByteBuf.refCnt());
        Assert.assertEquals("drc1", newFilterLogEvent.getSchemaName());
        Assert.assertEquals(101, newFilterLogEvent.getNextTransactionOffset());
        Assert.assertEquals("table1", newFilterLogEvent.getLastTableName());
        Assert.assertEquals(false, newFilterLogEvent.isNoRowsEvent());
        Assert.assertEquals(null, newFilterLogEvent.getRowsEventCount());
        Assert.assertEquals(count, newFilterLogEvent.getEventCount());
    }

    @Test
    public void testGetSchemaName() {
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", FilterLogEvent.UNKNOWN, 10, 10, 100);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals("drc1", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", "table1", 10, 5, 100);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals("drc1", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", "dly_db1", 10, 5, 100);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals("drc1", filterLogEvent.getSchemaNameLowerCaseV2());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", FilterLogEvent.UNKNOWN, 10, 10, 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaNameLowerCaseV2());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", "dly_db1", 20, 10, 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("db1", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", "dly_", 20, 10, 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaNameLowerCaseV2());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", "delaymonitordb", 20, 10, 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", "DLY_DB1", 20, 10, 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("db1", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("DRCMONITORDB", "DLY_DB1", 20, 10, 100);
        Assert.assertEquals("DRCMONITORDB", filterLogEvent.getSchemaName());
        Assert.assertEquals("db1", filterLogEvent.getSchemaNameLowerCaseV2());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("DRC1", "TABLE1", 20, 10, 100);
        Assert.assertEquals("DRC1", filterLogEvent.getSchemaName());
        Assert.assertEquals("drc1", filterLogEvent.getSchemaNameLowerCaseV2());

    }

    @Test
    public void testRowsEventNumNull() {
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("DRC1", "TABLE1", 20, 0, 100);
        Assert.assertFalse(filterLogEvent.isNoRowsEvent());
        Assert.assertEquals(Integer.valueOf(0), filterLogEvent.getRowsEventCount());

        filterLogEvent.encode(FilterLogEvent.UNKNOWN, FilterLogEvent.UNKNOWN, 20, 0, 100);
        Assert.assertTrue(filterLogEvent.isNoRowsEvent());
        Assert.assertEquals(Integer.valueOf(0), filterLogEvent.getRowsEventCount());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("DRC1", "TABLE1", 20, 10, 100);
        Assert.assertFalse(filterLogEvent.isNoRowsEvent());
        Assert.assertEquals(Integer.valueOf(10), filterLogEvent.getRowsEventCount());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("DRC1", "TABLE1", 20, null, 100);
        Assert.assertFalse(filterLogEvent.isNoRowsEvent());
        Assert.assertEquals(null, filterLogEvent.getRowsEventCount());
    }

    @Test
    public void testLoadV1() {
        byte[] bytes = {
                (byte) 0xcc, (byte) 0xad, (byte) 0x0f, (byte) 0x67, (byte) 0x6d, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x28, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x65, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x04, (byte) 0x64, (byte) 0x72, (byte) 0x63, (byte) 0x31, (byte) 0x65, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x75, (byte) 0x6E, (byte) 0x6B, (byte) 0x6E, (byte) 0x6F, (byte) 0x77,
                (byte) 0x6E, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        };
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        LogEventMockUtils.loadEventFromBytes(filterLogEvent, bytes);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals(FilterLogEvent.UNKNOWN, filterLogEvent.getLastTableName());
        Assert.assertEquals(101, filterLogEvent.getNextTransactionOffset());
        Assert.assertEquals(0, filterLogEvent.getEventCount());
        Assert.assertNull(filterLogEvent.getRowsEventCount());
    }

    @Test
    public void testLoadV2() {
        byte[] bytes = new byte[]{
                (byte) 0x89, (byte) 0xb0, (byte) 0x0f, (byte) 0x67, (byte) 0x6d, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x2c, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x65, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x04, (byte) 0x64, (byte) 0x72, (byte) 0x63, (byte) 0x31, (byte) 0x65, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x55, (byte) 0x4E, (byte) 0x4B, (byte) 0x4E, (byte) 0x4F, (byte) 0x4E,
                (byte) 0x57, (byte) 0x0A, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        };
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        LogEventMockUtils.loadEventFromBytes(filterLogEvent, bytes);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals("UNKNONW", filterLogEvent.getLastTableName());
        Assert.assertEquals(101, filterLogEvent.getNextTransactionOffset());
        Assert.assertEquals(10, filterLogEvent.getEventCount());
        Assert.assertEquals(Integer.valueOf(5), filterLogEvent.getRowsEventCount());
    }
}