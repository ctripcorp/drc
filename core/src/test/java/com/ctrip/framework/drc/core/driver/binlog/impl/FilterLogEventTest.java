package com.ctrip.framework.drc.core.driver.binlog.impl;

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
        filterLogEvent.encode("drc1", 101);
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
        filterLogEvent.encode("drc1", "table1", count, 101);
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
        Assert.assertEquals(count, newFilterLogEvent.getEventCount());
    }

    @Test
    public void testGetSchemaName() {
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", 100);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals("drc1", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", "table1", 10, 100);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals("drc1", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", "dly_db1", 10, 100);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals("drc1", filterLogEvent.getSchemaNameLowerCaseV2());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaNameLowerCaseV2());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", "dly_db1", 20, 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("db1", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", "dly_", 20, 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaNameLowerCaseV2());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", "delaymonitordb", 20, 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drcmonitordb", "DLY_DB1", 20, 100);
        Assert.assertEquals("drcmonitordb", filterLogEvent.getSchemaName());
        Assert.assertEquals("db1", filterLogEvent.getSchemaNameLowerCaseV2());

        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("DRCMONITORDB", "DLY_DB1", 20, 100);
        Assert.assertEquals("DRCMONITORDB", filterLogEvent.getSchemaName());
        Assert.assertEquals("db1", filterLogEvent.getSchemaNameLowerCaseV2());


        filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("DRC1", "TABLE1", 20, 100);
        Assert.assertEquals("DRC1", filterLogEvent.getSchemaName());
        Assert.assertEquals("drc1", filterLogEvent.getSchemaNameLowerCaseV2());

    }
}