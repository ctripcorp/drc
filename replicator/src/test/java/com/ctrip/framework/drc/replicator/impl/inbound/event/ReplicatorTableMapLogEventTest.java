package com.ctrip.framework.drc.replicator.impl.inbound.event;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.replicator.MockTest;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;

/**
 * @Author limingdong
 * @create 2020/6/23
 */
public class ReplicatorTableMapLogEventTest extends MockTest {

    @Mock
    private IoCache ioCache;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
    }

    @Test
    public void testWrite() {
        ByteBuf byteBuf = initLatin1CharsetByteBuf();
        TableMapLogEvent tableMapLogEvent = new ReplicatorTableMapLogEvent().read(byteBuf);
        tableMapLogEvent.write(ioCache);
        verify(ioCache, times(1)).write(Lists.newArrayList(tableMapLogEvent.getLogEventHeader().getHeaderBuf(), tableMapLogEvent.getPayloadBuf()));
    }

    @Test
    public void testReadReplicatorTableMapLogEvent() {
        ByteBuf byteBuf = initLatin1CharsetByteBuf();
        TableMapLogEvent tableMapLogEvent = new ReplicatorTableMapLogEvent().read(byteBuf);
        if (null == tableMapLogEvent) {
            Assert.fail();
        }

        if (null == tableMapLogEvent.getLogEventHeader()) {
            Assert.fail();
        }

        // valid decode
        Assert.assertEquals(table_map_log_event, LogEventType.getLogEventType(tableMapLogEvent.getLogEventHeader().getEventType()));
        Assert.assertEquals(123, tableMapLogEvent.getTableId());
        Assert.assertEquals(1, tableMapLogEvent.getFlags());
        Assert.assertEquals("gtid_test", tableMapLogEvent.getSchemaName());
        Assert.assertEquals("row_image3", tableMapLogEvent.getTableName());
        Assert.assertEquals(0, tableMapLogEvent.getColumnsCount());
        byteBuf.release();
        tableMapLogEvent.release();


        byteBuf = initLatin1CharsetByteBuf();
        tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        if (null == tableMapLogEvent) {
            Assert.fail();
        }

        if (null == tableMapLogEvent.getLogEventHeader()) {
            Assert.fail();
        }

        // valid decode
        Assert.assertEquals(table_map_log_event, LogEventType.getLogEventType(tableMapLogEvent.getLogEventHeader().getEventType()));
        Assert.assertEquals(123, tableMapLogEvent.getTableId());
        Assert.assertEquals(1, tableMapLogEvent.getFlags());
        Assert.assertEquals("gtid_test", tableMapLogEvent.getSchemaName());
        Assert.assertEquals("row_image3", tableMapLogEvent.getTableName());
        Assert.assertEquals(4, tableMapLogEvent.getColumnsCount());

        final List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();
        // id
        final TableMapLogEvent.Column id = columns.get(0);
        Assert.assertEquals(3, id.getType());
        Assert.assertEquals(0, id.getMeta());
        Assert.assertTrue(id.isNullable());
        // name
        final TableMapLogEvent.Column name = columns.get(1);
        Assert.assertEquals(254, name.getType());
        Assert.assertTrue(name.isNullable());
        // age
        final TableMapLogEvent.Column age = columns.get(2);
        Assert.assertEquals(3, age.getType());
        Assert.assertEquals(0, age.getMeta());
        Assert.assertTrue(age.isNullable());
        // varchar
        final TableMapLogEvent.Column varchar = columns.get(3);
        Assert.assertEquals(15, varchar.getType());
        Assert.assertEquals(258, varchar.getMeta());
        Assert.assertTrue(varchar.isNullable());

        Assert.assertEquals("4d7a508e", Long.toHexString(tableMapLogEvent.getChecksum()));
        Assert.assertEquals(65, byteBuf.readerIndex());
        Assert.assertEquals(46, tableMapLogEvent.getPayloadBuf().readerIndex());
        Assert.assertEquals(19, tableMapLogEvent.getLogEventHeader().getHeaderBuf().readerIndex());

        byteBuf.release();
        tableMapLogEvent.release();
    }

    private ByteBuf initLatin1CharsetByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(65);
        byte[] bytes = new byte[]{
                (byte) 0x70, (byte) 0xe3, (byte) 0x7c, (byte) 0x5d, (byte) 0x13, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x6e, (byte) 0x03, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x09, (byte) 0x67, (byte) 0x74, (byte) 0x69, (byte) 0x64,

                (byte) 0x5f, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x00, (byte) 0x0a, (byte) 0x72,
                (byte) 0x6f, (byte) 0x77, (byte) 0x5f, (byte) 0x69, (byte) 0x6d, (byte) 0x61, (byte) 0x67, (byte) 0x65,

                (byte) 0x33, (byte) 0x00, (byte) 0x04, (byte) 0x03, (byte) 0xfe, (byte) 0x03, (byte) 0x0f, (byte) 0x04,
                (byte) 0xfe, (byte) 0x1e, (byte) 0x02, (byte) 0x01, (byte) 0x0f, (byte) 0x8e, (byte) 0x50, (byte) 0x7a,

                (byte) 0x4d
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

}