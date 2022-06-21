package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.ctrip.framework.drc.core.driver.binlog.impl.DrcHeartbeatLogEvent.APPLIER_TOUCH_PROGRESS;

/**
 * @Author limingdong
 * @create 2022/3/29
 */
public class DrcHeartbeatLogEventTest {

    private DrcHeartbeatLogEvent drcHeartbeatLogEvent;

    private static final int CODE = 0;

    @Before
    public void setUp() throws Exception {
        drcHeartbeatLogEvent = new DrcHeartbeatLogEvent(CODE);
    }

    @Test
    public void readDrcHeartbeatLogEvent() {
        Assert.assertTrue(drcHeartbeatLogEvent.getLogEventType() == LogEventType.drc_heartbeat_log_event);
        ByteBuf headByteBuf = drcHeartbeatLogEvent.getLogEventHeader().getHeaderBuf();
        headByteBuf.readerIndex(0);

        ByteBuf payloadByteBuf =  drcHeartbeatLogEvent.getPayloadBuf();
        payloadByteBuf.readerIndex(0);

        DrcHeartbeatLogEvent clone = new DrcHeartbeatLogEvent();

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headByteBuf.readableBytes() + payloadByteBuf.readableBytes());
        compositeByteBuf.addComponents(true, headByteBuf, payloadByteBuf);

        clone.read(compositeByteBuf);

        Assert.assertEquals(clone.getCode(), CODE);
        Assert.assertFalse(clone.shouldTouchProgress());
    }

    @Test
    public void readDrcHeartbeatLogEventWithTouchProgress() {
        DrcHeartbeatLogEvent drcHeartbeatLogEvent = new DrcHeartbeatLogEvent(CODE, APPLIER_TOUCH_PROGRESS);
        Assert.assertTrue(drcHeartbeatLogEvent.getLogEventType() == LogEventType.drc_heartbeat_log_event);
        ByteBuf headByteBuf = drcHeartbeatLogEvent.getLogEventHeader().getHeaderBuf();
        headByteBuf.readerIndex(0);

        ByteBuf payloadByteBuf =  drcHeartbeatLogEvent.getPayloadBuf();
        payloadByteBuf.readerIndex(0);

        DrcHeartbeatLogEvent clone = new DrcHeartbeatLogEvent();

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headByteBuf.readableBytes() + payloadByteBuf.readableBytes());
        compositeByteBuf.addComponents(true, headByteBuf, payloadByteBuf);

        clone.read(compositeByteBuf);

        Assert.assertEquals(clone.getCode(), CODE);
        Assert.assertTrue(clone.shouldTouchProgress());
    }

}