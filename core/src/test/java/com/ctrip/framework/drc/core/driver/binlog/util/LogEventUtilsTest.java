package com.ctrip.framework.drc.core.driver.binlog.util;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.previous_gtids_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.update_rows_event_v2;

/**
 * Created by @author zhuYongMing on 2019/9/27.
 */
public class LogEventUtilsTest {

    @Test
    public void parseNextLogEventTypeTest() {
        final ByteBuf byteBuf = initByteBuf();
        final LogEventType logEventType = LogEventUtils.parseNextLogEventType(byteBuf);

        // valid
        Assert.assertEquals(update_rows_event_v2, logEventType);
        Assert.assertEquals(0, byteBuf.readerIndex());
    }

    @Test
    public void parseNextLogEventSizeTest() {
        final ByteBuf byteBuf = initByteBuf();
        final long eventSize = LogEventUtils.parseNextLogEventSize(byteBuf);

        Assert.assertEquals(64, eventSize);
        Assert.assertEquals(0, byteBuf.readerIndex());
    }

    @Test
    public void testDrcTableMapLogEvent() {
        LogEventType eventType = LogEventType.drc_table_map_log_event;
        Assert.assertTrue(LogEventUtils.isDrcTableMapLogEvent(eventType));
        eventType = LogEventType.drc_gtid_log_event;
        Assert.assertTrue(LogEventUtils.isDrcGtidLogEvent(eventType));
        Assert.assertTrue(LogEventUtils.isGtidLogEvent(eventType));
        eventType = LogEventType.gtid_log_event;
        Assert.assertTrue(LogEventUtils.isOriginGtidLogEvent(eventType));
        Assert.assertTrue(LogEventUtils.isGtidLogEvent(eventType));
    }

    @Test
    public void testDrcDdlLogEvent() {
        LogEventType eventType = LogEventType.drc_ddl_log_event;
        Assert.assertTrue(LogEventUtils.isDrcDdlLogEvent(eventType));
        Assert.assertTrue(LogEventUtils.isDdlEvent(eventType));
    }

    @Test
    public void testReplicatorSlaveConcerned() {
        LogEventType eventType = LogEventType.drc_schema_snapshot_log_event;
        Assert.assertTrue(LogEventUtils.isSlaveConcerned(eventType));
    }

    @Test
    public void testDrcEvent() {
        LogEventType eventType = LogEventType.drc_schema_snapshot_log_event;
        Assert.assertTrue(LogEventUtils.isDrcEvent(eventType));
        Assert.assertFalse(LogEventUtils.isDrcEvent(previous_gtids_log_event));
    }

    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(19);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x1f, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x40, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xd6, (byte) 0x2f, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
