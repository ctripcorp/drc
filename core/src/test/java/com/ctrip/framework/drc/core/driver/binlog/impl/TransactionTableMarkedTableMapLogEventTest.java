package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;

/**
 * @Author limingdong
 * @create 2022/10/12
 */
public class TransactionTableMarkedTableMapLogEventTest {

    @Test
    public void readLatin1CharsetTest() {
        final ByteBuf byteBuf = initLatin1CharsetByteBuf();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        TransactionTableMarkedTableMapLogEvent delegate = new TransactionTableMarkedTableMapLogEvent(tableMapLogEvent);

        // valid decode
        Assert.assertEquals(table_map_log_event, LogEventType.getLogEventType(delegate.getLogEventHeader().getEventType()));
        Assert.assertEquals(123, delegate.getTableId());
        Assert.assertEquals(1, delegate.getFlags());
        Assert.assertEquals("gtid_test", delegate.getSchemaName());
        Assert.assertEquals("row_image3", delegate.getTableName());
        Assert.assertEquals(4, delegate.getColumnsCount());

        final List<TableMapLogEvent.Column> columns = delegate.getColumns();
        // id
        final TableMapLogEvent.Column id = columns.get(0);
        Assert.assertEquals(3, id.getType());
        Assert.assertEquals(0, id.getMeta());
        Assert.assertTrue(id.isNullable());
        // name
        final TableMapLogEvent.Column name = columns.get(1);
        Assert.assertEquals(254, name.getType());
//        Assert.assertEquals(65054, name.getMeta());
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

        Assert.assertEquals("4d7a508e", Long.toHexString(delegate.getChecksum()));
        Assert.assertEquals(65, byteBuf.readerIndex());
        Assert.assertEquals(46, delegate.getPayloadBuf().readerIndex());
        Assert.assertEquals(19, delegate.getLogEventHeader().getHeaderBuf().readerIndex());
    }

    @Test
    public void constructDrcTableMapLogEventAndWriteTest() throws IOException, InterruptedException {
        List<TableMapLogEvent.Column> columns = mockColumns();
        final TableMapLogEvent delegate = new TableMapLogEvent(
                1L, 813, 123, "gtid_test", "row_image3", columns, null
        );
        TransactionTableMarkedTableMapLogEvent constructorTableMapLogEvent = new TransactionTableMarkedTableMapLogEvent(delegate);


        final ByteBuf writeByteBuf = ByteBufAllocator.DEFAULT.directBuffer();
        constructorTableMapLogEvent.write(byteBufs -> {
            for (ByteBuf byteBuf : byteBufs) {
                final byte[] bytes = new byte[byteBuf.writerIndex()];
                for (int i = 0; i < byteBuf.writerIndex(); i++) {
                    bytes[i] = byteBuf.getByte(i);
                }
                writeByteBuf.writeBytes(bytes);
            }
        });

        TimeUnit.SECONDS.sleep(1);
        final TransactionTableMarkedTableMapLogEvent readTableMapLogEvent = new TransactionTableMarkedTableMapLogEvent().read(writeByteBuf);
        Assert.assertEquals(constructorTableMapLogEvent, readTableMapLogEvent);
    }

    /**
     * no extra data
     * <p>
     * mysql> desc row_image3;
     * +---------+-------------+------+-----+---------+-------+
     * | Field   | Type        | Null | Key | Default | Extra |
     * +---------+-------------+------+-----+---------+-------+
     * | id      | int(11)     | YES  |     | NULL    |       |
     * | name    | char(10)    | YES  |     | NULL    |       |
     * | age     | int(10)     | YES  |     | NULL    |       |
     * | varchar | varchar(86) | YES  |     | NULL    |       |
     * +---------+-------------+------+-----+---------+-------+
     * <p>
     * # at 813
     * #190914 20:56:16 server id 1  end_log_pos 878 CRC32 0x4d7a508e  Table_map: `gtid_test`.`row_image3` mapped to number 123
     * <p>
     * 0000032d  70 e3 7c 5d 13 01 00 00  00 41 00 00 00 6e 03 00  |p.|].....A...n..|
     * 0000033d  00 00 00 7b 00 00 00 00  00 01 00 09 67 74 69 64  |...{........gtid|
     * 0000034d  5f 74 65 73 74 00 0a 72  6f 77 5f 69 6d 61 67 65  |_test..row_image|
     * 0000035d  33 00 04 03 fe 03 0f 04  fe 1e 02 01 0f 8e 50 7a 4d
     */
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

    public static List<TableMapLogEvent.Column> mockColumns() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, "default");
        Assert.assertFalse(column1.isOnUpdate());
        Assert.assertFalse(column1.isPk());
        Assert.assertFalse(column1.isUk());
        Assert.assertEquals("default", column1.getColumnDefault());
        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("name", true, "char", "30", null, null, null, "UTF8", "utf8_unicode_ci", "char(10)", "PRI", null, null);
        Assert.assertFalse(column2.isOnUpdate());
        Assert.assertTrue(column2.isPk());
        Assert.assertFalse(column2.isUk());
        Assert.assertEquals(null, column2.getColumnDefault());
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("age", true, "int", null, "10", null, null, null, null, "int(10)", "UNI", null, "one");
        Assert.assertFalse(column3.isOnUpdate());
        Assert.assertFalse(column3.isPk());
        Assert.assertTrue(column3.isUk());
        Assert.assertEquals("one", column3.getColumnDefault());
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("varchar", true, "varchar", "258", null, null, null, "UTF8", "utf8_unicode_ci", "varchar(86)", "MUL", null, "two");
        Assert.assertFalse(column4.isOnUpdate());
        Assert.assertFalse(column4.isPk());
        Assert.assertFalse(column4.isUk());
        Assert.assertEquals("two", column4.getColumnDefault());

        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columns.add(column4);

        return columns;
    }

}