package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.EMPTY_DRC_UUID_EVENT_SIZE;

/**
 * @Author limingdong
 * @create 2020/12/31
 */
public class DrcUuidLogEventTest {

    private Set<String> uuids = Sets.newHashSet();

    private DrcUuidLogEvent drcUuidLogEvent;

    @Before
    public void setUp() throws Exception {
        for(int i = 0; i < 10; ++i) {
            uuids.add(UUID.randomUUID().toString());
        }

        drcUuidLogEvent = new DrcUuidLogEvent(uuids, 0 , 10);

    }

    @After
    public void tearDown() throws Exception {
        drcUuidLogEvent.release();
    }

    @Test
    public void testEmptyDrcUuidLogEvent() {
        DrcUuidLogEvent clone = new DrcUuidLogEvent(Sets.newHashSet(), 0, 0);
        int size = clone.getLogEventHeader().getHeaderBuf().writerIndex() + clone.getPayloadBuf().writerIndex();
        Assert.assertEquals(size, EMPTY_DRC_UUID_EVENT_SIZE);
        clone.release();
    }

    @Test
    public void read() {
        ByteBuf headByteBuf = drcUuidLogEvent.getLogEventHeader().getHeaderBuf();
        headByteBuf.readerIndex(0);

        ByteBuf payloadByteBuf =  drcUuidLogEvent.getPayloadBuf();
        payloadByteBuf.readerIndex(0);

        DrcUuidLogEvent clone = new DrcUuidLogEvent();

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headByteBuf.readableBytes() + payloadByteBuf.readableBytes());
        compositeByteBuf.addComponents(true, headByteBuf, payloadByteBuf);

        DrcUuidLogEvent uuidLogEvent = (DrcUuidLogEvent) clone.read(compositeByteBuf);

        Set<String> cloneUuids = uuidLogEvent.getUuids();
        Assert.assertEquals(cloneUuids, uuids);
    }
}