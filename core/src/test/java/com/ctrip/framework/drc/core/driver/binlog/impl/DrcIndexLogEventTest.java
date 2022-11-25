package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.assertj.core.util.Lists;
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
        doTestDrcIndexLogEvent(drcIndexLogEvent, index, notRevisedIndices);
    }

    @Test
    public void testMaxValueInDrcIndexLogEvent() {
        List<Long> index = Lists.newArrayList();
        for (int i = 0; i < 11; ++i) {
            index.add(Long.MAX_VALUE);
        }

        DrcIndexLogEvent indexLogEvent = new DrcIndexLogEvent(index, index, 100, 100);
        doTestDrcIndexLogEvent(indexLogEvent, index, index);
    }

    private void doTestDrcIndexLogEvent(DrcIndexLogEvent indexLogEvent, List<Long> index, List<Long> notRevisedIndices) {
        ByteBuf headerByteBuf = indexLogEvent.getLogEventHeader().getHeaderBuf();
        headerByteBuf.readerIndex(0);
        ByteBuf payloadByteBuf = indexLogEvent.getPayloadBuf();
        payloadByteBuf.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, headerByteBuf, payloadByteBuf);

        DrcIndexLogEvent copy = new DrcIndexLogEvent();
        copy.read(compositeByteBuf);
        List<Long> copyIndex = copy.getIndices();
        for (int i = 0; i < copyIndex.size(); ++i) {
            Assert.assertEquals(copyIndex.get(i), index.get(i));
        }
        List<Long> copyNotRevisedIndices = copy.getNotRevisedIndices();
        for (int i = 0; i < copyNotRevisedIndices.size(); ++i) {
            Assert.assertEquals(copyNotRevisedIndices.get(i), notRevisedIndices.get(i));
        }
    }
}