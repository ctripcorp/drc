package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_DELAY_MONITOR_TABLE_NAME;

/**
 * @Author limingdong
 * @create 2020/4/24
 */
public class DelayMonitorFilterTest extends AbstractFilterTest {

    private DelayMonitorFilter delayMonitorFilter;

    @Mock
    private TableMapLogEvent tableMapLogEvent;

    @Mock
    private UpdateRowsEvent updateRowsEvent;

    @Mock
    private LogEventHeader logEventHeader;

    @Mock
    private ByteBuf payload;

    @Mock
    private ByteBuf head;

    private DefaultMonitorManager delayMonitor = new DefaultMonitorManager();

    @Mock
    private MonitorEventObserver monitorEventObserver;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        delayMonitorFilter = new DelayMonitorFilter(delayMonitor);
    }

    @Test
    public void doFilterTrue() {
        when(tableMapLogEvent.getTableName()).thenReturn(DRC_DELAY_MONITOR_TABLE_NAME);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        logEventWithGroupFlag.setInExcludeGroup(true);
        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);

        boolean skip = delayMonitorFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        logEventWithGroupFlag.releaseEvent();
        verify(tableMapLogEvent, times(1)).release();

        logEventWithGroupFlag.setInExcludeGroup(true);
        logEventWithGroupFlag.setLogEvent(updateRowsEvent);
        when(updateRowsEvent.getLogEventType()).thenReturn(LogEventType.update_rows_event_v2);
        when(updateRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(updateRowsEvent.getPayloadBuf()).thenReturn(payload);
        when(logEventHeader.getHeaderBuf()).thenReturn(head);
        skip = delayMonitorFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        logEventWithGroupFlag.releaseEvent();
        verify(updateRowsEvent, times(1)).release();
    }

    @Test
    public void testAdd() {
        delayMonitor.addObserver(monitorEventObserver);
        when(tableMapLogEvent.getTableName()).thenReturn(DRC_DELAY_MONITOR_TABLE_NAME);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        logEventWithGroupFlag.setInExcludeGroup(true);
        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);

        boolean skip = delayMonitorFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        logEventWithGroupFlag.releaseEvent();
        verify(tableMapLogEvent, times(1)).release();

        UpdateRowsEvent realUpdateRowsEvent = new UpdateRowsEvent().read(initByteBuf());
        logEventWithGroupFlag.setInExcludeGroup(true);
        logEventWithGroupFlag.setLogEvent(realUpdateRowsEvent);

        skip = delayMonitorFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        logEventWithGroupFlag.releaseEvent();
        verify(updateRowsEvent, times(0)).release();
    }

    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(98);
        byte[] bytes = new byte[] {
                (byte) 0x8e, (byte) 0x87, (byte) 0x99, (byte) 0x5e, (byte) 0x1f, (byte) 0x64, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x62, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xe8, (byte) 0x35, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0xff, (byte) 0xff, (byte) 0xf0, (byte) 0x02, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5f, (byte) 0x6f, (byte) 0x79, (byte) 0x07, (byte) 0x74,
                (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5f, (byte) 0x6f, (byte) 0x79, (byte) 0x5e, (byte) 0x99, (byte) 0x87, (byte) 0x77, (byte) 0x1c, (byte) 0xfc, (byte) 0xf0, (byte) 0x02, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5f, (byte) 0x6f, (byte) 0x79, (byte) 0x07, (byte) 0x74, (byte) 0x65,
                (byte) 0x73, (byte) 0x74, (byte) 0x5f, (byte) 0x6f, (byte) 0x79, (byte) 0x5e, (byte) 0x99, (byte) 0x87, (byte) 0x8e, (byte) 0x10, (byte) 0xcc, (byte) 0x17, (byte) 0xe7, (byte) 0x89, (byte) 0x3f
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

}
