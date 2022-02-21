package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.replicator.MockTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DOT;

/**
 * @Author limingdong
 * @create 2020/2/24
 */
public class BlackTableNameFilterTest extends MockTest {

    private static final String BLACK_DB = "ghostdb";

    private static final String QMQ_TABLE = "qmq_msg_queue";

    private BlackTableNameFilter tableNameFilter;

    private LogEventWithGroupFlag value;

    private TableMapLogEvent tableMapLogEvent;

    @Mock
    private InboundMonitorReport inboundMonitorReport;

    @Before
    public void setUp() {
        super.initMocks();
        tableNameFilter = new BlackTableNameFilter(inboundMonitorReport, Sets.newHashSet());
        tableNameFilter.getEXCLUDED_TABLE().add(QMQ_TABLE);

        ByteBuf byteBuf = getTableMapLogEvent();
        tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        byteBuf.release();

        value = new LogEventWithGroupFlag(tableMapLogEvent, false, false, false, "");
    }

    @After
    public void tearDown() {
        tableMapLogEvent.release();
    }

    @Test
    public void doGhostFilter() {
        Assert.assertTrue(tableNameFilter.doFilter(value));
        Assert.assertTrue(value.isInExcludeGroup());
        Assert.assertTrue(value.isTableFiltered());
    }

    @Test
    public void doBlackDbFilter() {
        tableNameFilter.getEXCLUDED_DB().add(BLACK_DB);
        Assert.assertTrue(tableNameFilter.doFilter(value));
        Assert.assertTrue(value.isInExcludeGroup());
        Assert.assertTrue(value.isTableFiltered());
    }

    @Test
    public void testQmqFilter() throws IOException {
        String testDbName = "testDbName";
        String testTableName = "testTableName";

        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(
                1L, 813, 123, testDbName, QMQ_TABLE, Lists.newArrayList(), null, table_map_log_event, 0
        );
        LogEventWithGroupFlag eventWithGroupFlag = new LogEventWithGroupFlag(tableMapLogEvent, false, false, false, "");
        Assert.assertTrue(tableNameFilter.doFilter(eventWithGroupFlag));
        Assert.assertTrue(eventWithGroupFlag.isInExcludeGroup());
        Assert.assertTrue(eventWithGroupFlag.isTableFiltered());

        tableNameFilter.getEXCLUDED_CUSTOM_TABLE().add(testDbName + DOT + testTableName);
        tableMapLogEvent = new TableMapLogEvent(
                1L, 813, 123, testDbName, testTableName, Lists.newArrayList(), null, table_map_log_event, 0
        );
        eventWithGroupFlag = new LogEventWithGroupFlag(tableMapLogEvent, false, false, false, "");
        Assert.assertTrue(tableNameFilter.doFilter(eventWithGroupFlag));
        Assert.assertTrue(eventWithGroupFlag.isInExcludeGroup());
        Assert.assertTrue(eventWithGroupFlag.isTableFiltered());

    }

    /**
     * #     380a d3 85 53 5e   13   64 00 00 00   41 00 00 00   ce f6 30 00   00 00
     * #     381d 8a 02 00 00 00 00 01 00  07 67 68 6f 73 74 64 62 |.........ghostdb|
     * #     382d 00 0b 5f 74 65 73 74 31  67 5f 67 68 63 00 04 08 |...test1g.ghc...|
     * #     383d 11 0f 0f 05 00 40 00 00  10 00 2a d1 db 56       |.............V|
     *
     * @return
     */
    private ByteBuf getTableMapLogEvent() {
        return initByteBuf();
    }

    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(35);
        byte[] bytes = new byte[]{
                (byte) 0xd3, (byte) 0x85, (byte) 0x53, (byte) 0x5e, (byte) 0x13, (byte) 0x64, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xce, (byte) 0xf6, (byte) 0x30,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x8a, (byte) 0x02, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x07, (byte) 0x67, (byte) 0x68, (byte) 0x6f, (byte) 0x73,


                (byte) 0x74, (byte) 0x64, (byte) 0x62, (byte) 0x00, (byte) 0x0b, (byte) 0x5f, (byte) 0x74, (byte) 0x65,
                (byte) 0x73, (byte) 0x74, (byte) 0x31, (byte) 0x67, (byte) 0x5f, (byte) 0x67, (byte) 0x68, (byte) 0x63,

                (byte) 0x00, (byte) 0x04, (byte) 0x08, (byte) 0x11, (byte) 0x0f, (byte) 0x0f, (byte) 0x05, (byte) 0x00,
                (byte) 0x40, (byte) 0x00, (byte) 0x00, (byte) 0x10, (byte) 0x00, (byte) 0x2a, (byte) 0xd1, (byte) 0xdb,


                (byte) 0x56
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
