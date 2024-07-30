package com.ctrip.framework.drc.core.driver.binlog.constant;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.unknown_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.update_rows_event_v2;

/**
 * Created by @author zhuYongMing on 2019/9/12.
 */
public class LogEventTypeTest {

    @Test
    public void getLogEventTypeTest() {
        Assert.assertEquals(update_rows_event_v2, LogEventType.getLogEventType(31));
        for (LogEventType value : LogEventType.values()) {
            Assert.assertEquals(value, LogEventType.getLogEventType(value.getType()));
        }
        Assert.assertEquals(unknown_log_event, LogEventType.getLogEventType(9191));
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
