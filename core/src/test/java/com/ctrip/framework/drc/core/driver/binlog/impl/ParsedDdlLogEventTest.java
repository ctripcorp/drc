package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-12-30
 */
public class ParsedDdlLogEventTest {

    private static final String schema = "testdb";

    private static final String table = "customer";

    private static final String TRUNCATE_DDL = "truncate table customer";

    private ParsedDdlLogEvent parsedDdlLogEvent;

    @Before
    public void setUp() throws IOException {
        parsedDdlLogEvent = new ParsedDdlLogEvent(schema, table, TRUNCATE_DDL, QueryType.TRUNCATE);
    }

    @Test
    public void read() {
        ByteBuf headerBuf = parsedDdlLogEvent.getLogEventHeader().getHeaderBuf();
        headerBuf.readerIndex(0);

        ByteBuf payloadBuf = parsedDdlLogEvent.getPayloadBuf();
        payloadBuf.readerIndex(0);

        ParsedDdlLogEvent clone = new ParsedDdlLogEvent();

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headerBuf.readableBytes() + payloadBuf.readableBytes());
        compositeByteBuf.addComponents(true, headerBuf, payloadBuf);

        clone.read(compositeByteBuf);

        Assert.assertEquals(clone.getSchema(), parsedDdlLogEvent.getSchema());
        Assert.assertEquals(clone.getTable(), parsedDdlLogEvent.getTable());
        Assert.assertEquals(clone.getDdl(), parsedDdlLogEvent.getDdl());
        Assert.assertEquals(clone.getQueryType(), parsedDdlLogEvent.getQueryType());

        Assert.assertNotEquals(0, clone.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertNotEquals(0, clone.getPayloadBuf().refCnt());
        Assert.assertNotEquals(0, parsedDdlLogEvent.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertNotEquals(0, parsedDdlLogEvent.getPayloadBuf().refCnt());

        clone.release();
        parsedDdlLogEvent.release();
        Assert.assertEquals(0, clone.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(0, clone.getPayloadBuf().refCnt());
        Assert.assertEquals(0, parsedDdlLogEvent.getLogEventHeader().getHeaderBuf().refCnt());
        Assert.assertEquals(0, parsedDdlLogEvent.getPayloadBuf().refCnt());
    }
}
