package com.ctrip.framework.drc.replicator.store;

import com.ctrip.framework.drc.core.driver.binlog.impl.DrcDdlLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.replicator.MockTest;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;

/**
 * Created by mingdongli
 * 2019/9/18 下午2:40.
 */
public abstract class AbstractEventTest extends MockTest {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected static final String DESTINATION = "drctest1";

    protected static final String GTID = "a0a1fbb8-bdc8-11e9-96a0-fa163e7af2ad:66";

    protected static final String TRUNCATED_GTID = "a0a1fbb8-bdc8-11e9-96a0-fa163e7af2ad:67";

    protected static final int EMPTY_SCHEMA_EVENT_SIZE = 0;

    protected int GTID_ZISE;

    protected int TABLE_MAP_SIZE;

    protected int WRITE_ROW_SIZE;

    protected int XID_ZISE;

    protected ByteBuf getPreviousGtidEvent() {
        String hexString =
                "7d a3 8d 5d 23 10 27 00  00 b7 00 00 00 32 01 00" +
                        "00 80 00 03 00 00 00 00  00 00 00 5f 9a 18 06 e0" +
                        "24 11 e9 85 88 fa 16 3e  7a f2 aa 01 00 00 00 00" +
                        "00 00 00 01 00 00 00 00  00 00 00 04 00 00 00 00" +
                        "00 00 00 5f 9a 18 06 e0  24 11 e9 85 88 fa 16 3e" +
                        "7a f2 ac 03 00 00 00 00  00 00 00 01 00 00 00 00" +
                        "00 00 00 04 00 00 00 00  00 00 00 05 00 00 00 00" +
                        "00 00 00 06 00 00 00 00  00 00 00 07 00 00 00 00" +
                        "00 00 00 09 00 00 00 00  00 00 00 5f 9a 18 06 e0" +
                        "24 11 e9 85 88 fa 16 3e  7a f2 ad 01 00 00 00 00" +
                        "00 00 00 01 00 00 00 00  00 00 00 13 00 00 00 00" +
                        "00 00 00 a5 bf c3 52";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    protected ByteBuf getGtidEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(65);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0xa1, (byte) 0xfb, (byte) 0xb8,
                (byte) 0xbd, (byte) 0xc8, (byte) 0x11, (byte) 0xe9, (byte) 0x96, (byte) 0xa0, (byte) 0xfa, (byte) 0x16,

                (byte) 0x3e, (byte) 0x7a, (byte) 0xf2, (byte) 0xad, (byte) 0x42, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x1e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40, (byte) 0xe9, (byte) 0xd9,

                (byte) 0x34
        };
        byteBuf.writeBytes(bytes);
        GTID_ZISE = bytes.length;
//        GTID_ZISE += 4;  // custom transaction size

        return byteBuf;
    }
    protected ByteBuf getGtidEventHearder() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(19);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x2e, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00
        };
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    protected ByteBuf getPartialGtidEvent() {  //0x43  67
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(65);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x2e, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0xa1, (byte) 0xfb, (byte) 0xb8,
                (byte) 0xbd, (byte) 0xc8, (byte) 0x11, (byte) 0xe9, (byte) 0x96, (byte) 0xa0, (byte) 0xfa, (byte) 0x16,

                (byte) 0x3e, (byte) 0x7a, (byte) 0xf2, (byte) 0xad, (byte) 0x43, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x1e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40, (byte) 0xe9, (byte) 0xd9,

                (byte) 0x34
        };
        byteBuf.writeBytes(bytes);
        GTID_ZISE = bytes.length;

        return byteBuf;
    }

    protected ByteBuf getCharsetTypeTableMapEvent() {
        String hexString =
                "20 84 a5 5d 13 01 00 00  00 55 00 00 00 1b 41 00" +
                        "00 00 00 5a 04 00 00 00  00 01 00 04 64 72 63 34" +
                        "00 0c 63 68 61 72 73 65  74 5f 74 79 70 65 00 0d" +
                        "0f fe 0f fe fc fc fc fc  fc fc fc fc fc 11 a0 0f" +
                        "ce e8 08 07 fe c8 01 03  02 04 01 03 02 04 04 ff" +
                        "1f 99 e0 cd dd";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);
        TABLE_MAP_SIZE = bytes.length;

        return byteBuf;
    }

    protected TableMapLogEvent getFilteredTableMapLogEvent(String dbName, String tableName, long tableId) throws IOException {
        List<TableMapLogEvent.Column> columns = mockColumns();
        TableMapLogEvent constructorTableMapLogEvent = new TableMapLogEvent(
                1L, 813, tableId, dbName, tableName, columns, null, table_map_log_event, 0
        );
        return constructorTableMapLogEvent;
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


    // is not STMT_END_F, table id is 0x7c(124)
    protected ByteBuf getMinimalRowsEventByteBufWithEndOfStatement() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byte[] bytes = new byte[]{
                (byte) 0x91, (byte) 0x5d, (byte) 0x7e, (byte) 0x5d, (byte) 0x1e, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x31, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xbd, (byte) 0x06, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x7c, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0x09, (byte) 0xfc,

                (byte) 0x09, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x00, (byte) 0x76, (byte) 0x61,
                (byte) 0x72, (byte) 0x63, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0xb8, (byte) 0x74, (byte) 0xe8,

                (byte) 0x80
        };
        byteBuf.writeBytes(bytes);

        WRITE_ROW_SIZE = bytes.length;

        return byteBuf;
    }


    // is not STMT_END_F, table id is 0x7c(124)
    protected ByteBuf getMinimalRowsEventByteBufWithNotEndOfStatement() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byte[] bytes = new byte[]{
                (byte) 0x91, (byte) 0x5d, (byte) 0x7e, (byte) 0x5d, (byte) 0x1e, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x31, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xbd, (byte) 0x06, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x7c, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0x09, (byte) 0xfc,

                (byte) 0x09, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x00, (byte) 0x76, (byte) 0x61,
                (byte) 0x72, (byte) 0x63, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0xb8, (byte) 0x74, (byte) 0xe8,

                (byte) 0x80
        };
        byteBuf.writeBytes(bytes);

        WRITE_ROW_SIZE = bytes.length;

        return byteBuf;
    }

    // is STMT_END_F, table id is 0x7b(123)
    protected ByteBuf getMinimalRowsEventByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byte[] bytes = new byte[]{
                (byte) 0x91, (byte) 0x5d, (byte) 0x7e, (byte) 0x5d, (byte) 0x1e, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x31, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xbd, (byte) 0x06, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0x09, (byte) 0xfc,

                (byte) 0x09, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x00, (byte) 0x76, (byte) 0x61,
                (byte) 0x72, (byte) 0x63, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0xb8, (byte) 0x74, (byte) 0xe8,

                (byte) 0x80
        };
        byteBuf.writeBytes(bytes);

        WRITE_ROW_SIZE = bytes.length;

        return byteBuf;
    }

    protected ByteBuf getMinimalRowsEventHeader() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byte[] bytes = new byte[]{
                (byte) 0x91, (byte) 0x5d, (byte) 0x7e, (byte) 0x5d, (byte) 0x1e, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x31, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xbd, (byte) 0x06, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00
        };
        byteBuf.writeBytes(bytes);

        WRITE_ROW_SIZE = bytes.length;

        return byteBuf;
    }

    protected ByteBuf getXidEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(35);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x10, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xf5, (byte) 0x2f, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x44, (byte) 0x04, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x82, (byte) 0xe5, (byte) 0xc7, (byte) 0x3a
        };
        byteBuf.writeBytes(bytes);

        XID_ZISE = bytes.length;

        return byteBuf;
    }

    protected ByteBuf getXidEventHeader() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(35);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x10, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xf5, (byte) 0x2f, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    protected ByteBuf getBrokenXidEventHeader() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(35);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x10, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xf5, (byte) 0x2f, (byte) 0x00,

                (byte) 0x00
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    protected TableMapLogEvent getDrcTableMapLogEvent() {
        try {
            TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(0, 0, 0, "dbName", "tableName", Lists.newArrayList(), null);
            return tableMapLogEvent;
        } catch (IOException e) {
        }
        return null;
    }

    protected DrcDdlLogEvent getDrcDdlLogEvent() {
        try {
            DrcDdlLogEvent drcDdlLogEvent = new DrcDdlLogEvent("ddl", "schemaName", 0, 0);
            return drcDdlLogEvent;
        } catch (IOException e) {
        }
        return null;
    }

    protected void deleteFiles(File logDir) {
        File[] files = logDir.listFiles();
        for (File file : files) {
            file.delete();
        }
    }

    public static byte[] toBytesFromHexString(String hexString) {
        // mac control+command+G multi rows operation
        hexString = hexString.replaceAll(" ", "");
        hexString = hexString.replaceAll("\n", "");
        return HexUtils.fromHexString(hexString);
    }
}
