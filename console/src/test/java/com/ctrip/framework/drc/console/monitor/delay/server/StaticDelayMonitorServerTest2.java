package com.ctrip.framework.drc.console.monitor.delay.server;

import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.monitor.delay.task.PeriodicalUpdateDbTask;
import com.ctrip.framework.drc.console.monitor.delay.task.PeriodicalUpdateDbTaskV2;
import com.ctrip.framework.drc.core.driver.binlog.impl.DelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.column.DbDelayDto;
import com.ctrip.framework.drc.core.monitor.column.DbDelayMonitorColumn;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

/**
 * @author: yongnian
 * @create: 2024/7/9 13:21
 */
public class StaticDelayMonitorServerTest2 {
    @InjectMocks
    StaticDelayMonitorServer staticDelayMonitorServer = new StaticDelayMonitorServer(mock(DelayMonitorSlaveConfig.class), null, null, null, null, 30000);

    @Mock
    PeriodicalUpdateDbTask periodicalUpdateDbTask;
    @Mock
    PeriodicalUpdateDbTaskV2 periodicalUpdateDbTaskV2;

    byte[] dbDelayBytes = new byte[]{
            (byte) 0x51, (byte) 0x96, (byte) 0x5c, (byte) 0x65, (byte) 0x1f, (byte) 0xea, (byte) 0x0c, (byte) 0x00, (byte) 0x00, (byte) 0xd0, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xd7, (byte) 0xf1, (byte) 0xd0, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
            (byte) 0x30, (byte) 0x5D, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x03, (byte) 0xFF, (byte) 0xFF, (byte) 0x00, (byte) 0xD9, (byte) 0x41,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x45, (byte) 0x00, (byte) 0x7B, (byte) 0x22, (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74,
            (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22,
            (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31,
            (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x62, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F,
            (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x64, (byte) 0x5F, (byte) 0x64, (byte) 0x62, (byte) 0x31, (byte) 0x36, (byte) 0x32, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x96,
            (byte) 0x4F, (byte) 0x1C, (byte) 0x98, (byte) 0x00, (byte) 0xD9, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x45, (byte) 0x00, (byte) 0x7B, (byte) 0x22,
            (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22,
            (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F,
            (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x62, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E,
            (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x64, (byte) 0x5F, (byte) 0x64, (byte) 0x62, (byte) 0x31, (byte) 0x36,
            (byte) 0x32, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x96, (byte) 0x51, (byte) 0x0E, (byte) 0xB0, (byte) 0xD4, (byte) 0xAC, (byte) 0x61, (byte) 0xAB,
    };

    byte[] mhaDelayBytes = new byte[]{
            (byte) 0x01, (byte) 0x97, (byte) 0x5c, (byte) 0x65, (byte) 0x1f, (byte) 0xea, (byte) 0x0c, (byte) 0x00, (byte) 0x00, (byte) 0xa6, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x2a, (byte) 0x59, (byte) 0x2c, (byte) 0x22, (byte) 0x00, (byte) 0x00,
            (byte) 0x59, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0xFF, (byte) 0xFF, (byte) 0x00, (byte) 0x6E, (byte) 0x0D,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x2A, (byte) 0x00, (byte) 0x7B, (byte) 0x22,
            (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22,
            (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F,
            (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x97, (byte) 0x00, (byte) 0x05, (byte) 0x8C, (byte) 0x00, (byte) 0x6E,
            (byte) 0x0D, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x2A, (byte) 0x00, (byte) 0x7B,
            (byte) 0x22, (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A,
            (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E,
            (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x97, (byte) 0x01, (byte) 0x12, (byte) 0x3E, (byte) 0xD3,
            (byte) 0x10, (byte) 0x25, (byte) 0x99,
    };
    private static final String gtid = "abcde123-5678-1234-abcd-abcd1234abcd:123456789";

    private static final String idc = "ntgxh";

    private ByteBuf initByteBuf(byte[] bytes) {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(98);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(staticDelayMonitorServer.getConfig().getEndpoint()).thenReturn(new DefaultEndPoint("ip1", 8080));
    }

    @Test
    public void processMhaDelayEvent() {
        ByteBuf eventByteBuf = initByteBuf(mhaDelayBytes);
        DelayMonitorLogEvent mhaDelayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        DbDelayDto dbDelayDto = DbDelayMonitorColumn.tryParseEvent(mhaDelayMonitorLogEvent);
        Assert.assertNull(dbDelayDto);

        staticDelayMonitorServer.processDelayEvent(mhaDelayMonitorLogEvent);
        verify(periodicalUpdateDbTask, times(1)).getAndDeleteCommitTime(any());
        verify(periodicalUpdateDbTaskV2, never()).getAndDeleteCommitTime(any());
    }

    @Test
    public void processDbDelayEvent() {
        ByteBuf eventByteBuf = initByteBuf(dbDelayBytes);
        DelayMonitorLogEvent delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        DbDelayDto dbDelayDto = DbDelayMonitorColumn.tryParseEvent(delayMonitorLogEvent);
        Assert.assertNotNull(dbDelayDto);

        staticDelayMonitorServer.processDelayEvent(delayMonitorLogEvent);
        verify(periodicalUpdateDbTaskV2, times(1)).getAndDeleteCommitTime(any());
        verify(periodicalUpdateDbTask, never()).getAndDeleteCommitTime(any());
    }
}