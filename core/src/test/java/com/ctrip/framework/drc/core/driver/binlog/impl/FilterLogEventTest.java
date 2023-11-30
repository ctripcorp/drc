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
    }
}