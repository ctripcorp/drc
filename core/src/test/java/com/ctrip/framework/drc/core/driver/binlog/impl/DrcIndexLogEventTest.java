package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/9/3
 */
public class DrcIndexLogEventTest {

    private DrcIndexLogEvent drcIndexLogEvent;

    private List<Long> index;

    private List<Long> notRevisedIndices;

    @Before
    public void setUp() throws Exception {
        index = new ArrayList<>();
        notRevisedIndices = new ArrayList<>();
        index.add(0L);
        notRevisedIndices.add(0L);
        index.add(Long.MAX_VALUE / 2);
        notRevisedIndices.add(Long.MAX_VALUE / 2);
        index.add(Long.MAX_VALUE);
        notRevisedIndices.add(Long.MAX_VALUE);
        drcIndexLogEvent = new DrcIndexLogEvent(index, notRevisedIndices, 100, 100);
    }

    @Test
    public void getIndices() {
        ByteBuf headerByteBuf = drcIndexLogEvent.getLogEventHeader().getHeaderBuf();
        headerByteBuf.readerIndex(0);
        ByteBuf payloadByteBuf = drcIndexLogEvent.getPayloadBuf();
        payloadByteBuf.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, headerByteBuf, payloadByteBuf);

        DrcIndexLogEvent copy = new DrcIndexLogEvent();
        copy.read(compositeByteBuf);
        List<Long> copyIndex = copy.getIndices();
        for (int i = 0; i < copyIndex.size(); ++i) {
            Assert.assertEquals(copyIndex.get(i), index.get(i));
        }
        List<Long> notRevisedIndices = copy.getNotRevisedIndices();
        for (int i = 0; i < notRevisedIndices.size(); ++i) {
            Assert.assertEquals(notRevisedIndices.get(i), notRevisedIndices.get(i));
        }
    }
}