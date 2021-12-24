package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by mingdongli
 * 2019/10/24 上午11:35.
 */
public class DrcErrorLogEventTest {

    private ResultCode resultCode = ResultCode.REPLICATOR_NOT_READY;

    @Test
    public void readAndWrite() {
        DrcErrorLogEvent drcErrorLogEvent = new DrcErrorLogEvent(resultCode.getCode(), resultCode.getMessage());
        ByteBuf headerByteBuf = drcErrorLogEvent.getLogEventHeader().getHeaderBuf();
        ByteBuf payloadByteBuf = drcErrorLogEvent.getPayloadBuf();

        headerByteBuf.readerIndex(0);
        payloadByteBuf.readerIndex(0);

        CompositeByteBuf compositeByteBuf = ByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, headerByteBuf, payloadByteBuf);

        DrcErrorLogEvent clone = new DrcErrorLogEvent();
        clone.read(compositeByteBuf); //不改变指针

        Assert.assertEquals(drcErrorLogEvent.getErrorNumber(), clone.getErrorNumber());
        Assert.assertEquals(drcErrorLogEvent.getMessage(), clone.getMessage());

        Assert.assertNotEquals(0, drcErrorLogEvent.getPayloadBuf().readableBytes());
        Assert.assertEquals(0, clone.getPayloadBuf().readableBytes());
    }
}