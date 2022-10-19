package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType.*;

/**
 * Created by @author zhuYongMing on 2019/9/15.
 */
public class TableMapLogEventTest {

    @Test
    public void readLatin1CharsetTest() {
        final ByteBuf byteBuf = initLatin1CharsetByteBuf();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
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

        Assert.assertEquals("4d7a508e", Long.toHexString(tableMapLogEvent.getChecksum()));
        Assert.assertEquals(65, byteBuf.readerIndex());
        Assert.assertEquals(46, tableMapLogEvent.getPayloadBuf().readerIndex());
        Assert.assertEquals(19, tableMapLogEvent.getLogEventHeader().getHeaderBuf().readerIndex());
    }

    @Test
    public void constructDrcTableMapLogEventAndWriteTest() throws IOException, InterruptedException {
        List<TableMapLogEvent.Column> columns = mockColumns();
        final TableMapLogEvent constructorTableMapLogEvent = new TableMapLogEvent(
                1L, 813, 123, "gtid_test", "row_image3", columns, null
        );

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
        final TableMapLogEvent readTableMapLogEvent = new TableMapLogEvent().read(writeByteBuf);
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


    // japan
    @Test
    public void readCp932CharsetTest() {
        final ByteBuf byteBuf = initCp932CharsetByteBuf();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        final List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();
        // id
        final TableMapLogEvent.Column id = columns.get(0);
        Assert.assertEquals(mysql_type_long, MysqlFieldType.getMysqlFieldType(id.getType()));
        Assert.assertEquals(0, id.getMeta());
        Assert.assertFalse(id.isNullable());
        // string
        final TableMapLogEvent.Column string = columns.get(1);
        Assert.assertEquals(mysql_type_string, MysqlFieldType.getMysqlFieldType(string.getType()));
//        Assert.assertEquals(65044, string.getMeta());
        Assert.assertTrue(string.isNullable());
        // varchar
        final TableMapLogEvent.Column varchar = columns.get(2);
        Assert.assertEquals(mysql_type_varchar, MysqlFieldType.getMysqlFieldType(varchar.getType()));
        Assert.assertEquals(20, varchar.getMeta());
        Assert.assertTrue(varchar.isNullable());
        // string1
        final TableMapLogEvent.Column string1 = columns.get(3);
        Assert.assertEquals(mysql_type_string, MysqlFieldType.getMysqlFieldType(string1.getType()));
//        Assert.assertEquals(65044, string1.getMeta());
        Assert.assertTrue(string1.isNullable());
        // varchar1
        final TableMapLogEvent.Column varchar1 = columns.get(4);
        Assert.assertEquals(mysql_type_varchar, MysqlFieldType.getMysqlFieldType(varchar1.getType()));
        Assert.assertEquals(20, varchar1.getMeta());
        Assert.assertTrue(varchar1.isNullable());
        // string2
        final TableMapLogEvent.Column string2 = columns.get(5);
        Assert.assertEquals(mysql_type_string, MysqlFieldType.getMysqlFieldType(string2.getType()));
//        Assert.assertEquals(65044, string2.getMeta());
        Assert.assertTrue(string2.isNullable());
        // varchar2
        final TableMapLogEvent.Column varchar2 = columns.get(6);
        Assert.assertEquals(mysql_type_varchar, MysqlFieldType.getMysqlFieldType(varchar2.getType()));
        Assert.assertEquals(20, varchar2.getMeta());
        Assert.assertTrue(varchar2.isNullable());
    }

    /**
     * japan charset
     * mysql> desc charset_test2.charset_japan2;
     * +----------+------------------+------+-----+---------+----------------+
     * | Field    | Type             | Null | Key | Default | Extra          |
     * +----------+------------------+------+-----+---------+----------------+
     * | id       | int(10) unsigned | NO   | PRI | NULL    | auto_increment |
     * | string   | char(10)         | YES  |     | NULL    |                |
     * | varchar  | varchar(10)      | YES  |     | NULL    |                |
     * | string1  | char(10)         | YES  |     | NULL    |                |
     * | varchar1 | varchar(10)      | YES  |     | NULL    |                |
     * | string2  | char(10)         | YES  |     | NULL    |                |
     * | varchar2 | varchar(10)      | YES  |     | NULL    |                |
     * +----------+------------------+------+-----+---------+----------------+
     */
    private ByteBuf initCp932CharsetByteBuf() {
        String hexString =
                "62 33 9f 5d 13 01 00 00  00 54 00 00 00 d5 2a 00" +
                        "00 00 00 7c 00 00 00 00  00 01 00 0d 63 68 61 72" +
                        "73 65 74 5f 74 65 73 74  32 00 0e 63 68 61 72 73" +
                        "65 74 5f 6a 61 70 61 6e  32 00 07 03 fe 0f fe 0f" +
                        "fe 0f 0c fe 14 14 00 fe  14 14 00 fe 14 14 00 7e" +
                        "79 5f 32 00";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }


    /**
     * test for read numeric type tableMapEvent
     */
    @Test
    public void readNumericTypeTest() {
        final ByteBuf byteBuf = initNumericTypeByteBuf();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        final List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();

        final TableMapLogEvent.Column bit = columns.get(0);
        Assert.assertEquals(mysql_type_bit, MysqlFieldType.getMysqlFieldType(bit.getType()));

        final TableMapLogEvent.Column tinyint = columns.get(1);
        Assert.assertEquals(mysql_type_tiny, MysqlFieldType.getMysqlFieldType(tinyint.getType()));

        final TableMapLogEvent.Column smallint = columns.get(2);
        Assert.assertEquals(mysql_type_short, MysqlFieldType.getMysqlFieldType(smallint.getType()));

        final TableMapLogEvent.Column mediumint = columns.get(3);
        Assert.assertEquals(mysql_type_int24, MysqlFieldType.getMysqlFieldType(mediumint.getType()));

        final TableMapLogEvent.Column intType = columns.get(4);
        Assert.assertEquals(mysql_type_long, MysqlFieldType.getMysqlFieldType(intType.getType()));

        final TableMapLogEvent.Column integerType = columns.get(5);
        Assert.assertEquals(mysql_type_long, MysqlFieldType.getMysqlFieldType(integerType.getType()));

        final TableMapLogEvent.Column bigint = columns.get(6);
        Assert.assertEquals(mysql_type_longlong, MysqlFieldType.getMysqlFieldType(bigint.getType()));
    }

    /**
     * mysql> desc `drc4`.`multi_type_number`;
     * +-----------+---------------+------+-----+---------+-------+
     * | Field     | Type          | Null | Key | Default | Extra |
     * +-----------+---------------+------+-----+---------+-------+
     * | bit       | bit(64)       | YES  |     | NULL    |       |
     * | tinyint   | tinyint(5)    | YES  |     | NULL    |       |
     * | smallint  | smallint(10)  | YES  |     | NULL    |       |
     * | mediumint | mediumint(15) | YES  |     | NULL    |       |
     * | int       | int(20)       | YES  |     | NULL    |       |
     * | integer   | int(20)       | YES  |     | NULL    |       |
     * | bigint    | bigint(100)   | YES  |     | NULL    |       |
     * +-----------+---------------+------+-----+---------+-------+
     * 7 rows in set (0.00 sec)
     */
    private ByteBuf initNumericTypeByteBuf() {
        String hexString =
                "15 8b a0 5d 13 01 00 00  00 44 00 00 00 65 53 00" +
                        "00 00 00 ef 00 00 00 00  00 01 00 04 64 72 63 34" +
                        "00 11 6d 75 6c 74 69 5f  74 79 70 65 5f 6e 75 6d" +
                        "62 65 72 00 07 10 01 02  09 03 03 08 02 00 08 7f" +
                        "cb 2c a5 67";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    /**
     * test for read float point type tableMapEvent
     */
    @Test
    public void readFloatPointType() {
        final ByteBuf byteBuf = initFloatPointType();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        final List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();

        final TableMapLogEvent.Column real = columns.get(0);
        Assert.assertEquals(mysql_type_double, MysqlFieldType.getMysqlFieldType(real.getType()));
        Assert.assertEquals(8, real.getMeta());

        final TableMapLogEvent.Column real10_4 = columns.get(1);
        Assert.assertEquals(mysql_type_double, MysqlFieldType.getMysqlFieldType(real10_4.getType()));
        Assert.assertEquals(8, real10_4.getMeta());

        final TableMapLogEvent.Column a_double = columns.get(2);
        Assert.assertEquals(mysql_type_double, MysqlFieldType.getMysqlFieldType(a_double.getType()));
        Assert.assertEquals(8, a_double.getMeta());

        final TableMapLogEvent.Column double10_4 = columns.get(3);
        Assert.assertEquals(mysql_type_double, MysqlFieldType.getMysqlFieldType(double10_4.getType()));
        Assert.assertEquals(8, double10_4.getMeta());

        final TableMapLogEvent.Column a_float = columns.get(4);
        Assert.assertEquals(mysql_type_float, MysqlFieldType.getMysqlFieldType(a_float.getType()));
        Assert.assertEquals(4, a_float.getMeta());

        final TableMapLogEvent.Column float10_5 = columns.get(5);
        Assert.assertEquals(mysql_type_float, MysqlFieldType.getMysqlFieldType(float10_5.getType()));
        Assert.assertEquals(4, float10_5.getMeta());

        final TableMapLogEvent.Column decimal = columns.get(6);
        Assert.assertEquals(mysql_type_newdecimal, MysqlFieldType.getMysqlFieldType(decimal.getType()));
        final Pair<Integer, Integer> decimalPair = getDecimalPrecisionAndScale(decimal.getMeta());
        Assert.assertEquals(10, decimalPair.getKey().intValue());
        Assert.assertEquals(0, decimalPair.getValue().intValue());

        final TableMapLogEvent.Column decimal10_4 = columns.get(7);
        Assert.assertEquals(mysql_type_newdecimal, MysqlFieldType.getMysqlFieldType(decimal.getType()));
        final Pair<Integer, Integer> decimal10_4Pair = getDecimalPrecisionAndScale(decimal10_4.getMeta());
        Assert.assertEquals(10, decimal10_4Pair.getKey().intValue());
        Assert.assertEquals(4, decimal10_4Pair.getValue().intValue());

        final TableMapLogEvent.Column numeric = columns.get(8);
        Assert.assertEquals(mysql_type_newdecimal, MysqlFieldType.getMysqlFieldType(numeric.getType()));
        final Pair<Integer, Integer> numericPair = getDecimalPrecisionAndScale(numeric.getMeta());
        Assert.assertEquals(10, numericPair.getKey().intValue());
        Assert.assertEquals(0, numericPair.getValue().intValue());

        final TableMapLogEvent.Column numeric10_4 = columns.get(9);
        Assert.assertEquals(mysql_type_newdecimal, MysqlFieldType.getMysqlFieldType(numeric10_4.getType()));
        final Pair<Integer, Integer> numeric10_4Pair = getDecimalPrecisionAndScale(numeric10_4.getMeta());
        Assert.assertEquals(10, numeric10_4Pair.getKey().intValue());
        Assert.assertEquals(4, numeric10_4Pair.getValue().intValue());

        byteBuf.release();
        tableMapLogEvent.release();
    }

    private Pair<Integer, Integer> getDecimalPrecisionAndScale(final int meta) {
        return new Pair<>(meta >> 8, meta & 0xff);
    }


    /**
     * sql:
     * CREATE TABLE `drc4`.`float_type` (
     * `real` real,
     * `real10_4` real(10,4),
     * `double` double,
     * `double10_4` double(10,4),
     * `float` float,
     * `float10_4` float(10,4),
     * `decimal` decimal,
     * `decimal10_4` decimal(10,4),
     * `numeric` numeric,
     * `numeric10_4` numeric(10,4)
     * ) ENGINE=InnoDB;
     * <p>
     * mysql> desc `drc4`.`float_type`;
     * +-------------+---------------+------+-----+---------+-------+
     * | Field       | Type          | Null | Key | Default | Extra |
     * +-------------+---------------+------+-----+---------+-------+
     * | real        | double        | YES  |     | NULL    |       |
     * | real10_4    | double(10,4)  | YES  |     | NULL    |       |
     * | double      | double        | YES  |     | NULL    |       |
     * | double10_4  | double(10,4)  | YES  |     | NULL    |       |
     * | float       | float         | YES  |     | NULL    |       |
     * | float10_4   | float(10,4)   | YES  |     | NULL    |       |
     * | decimal     | decimal(10,0) | YES  |     | NULL    |       |
     * | decimal10_4 | decimal(10,4) | YES  |     | NULL    |       |
     * | numeric     | decimal(10,0) | YES  |     | NULL    |       |
     * | numeric10_4 | decimal(10,4) | YES  |     | NULL    |       |
     * +-------------+---------------+------+-----+---------+-------+
     * 10 rows in set (0.00 sec)
     */
    private ByteBuf initFloatPointType() {
        String hexString =
                "c5 57 a9 5d 13 01 00 00  00 4d 00 00 00 c0 e2 00" +
                        "00 00 00 6b 05 00 00 00  00 01 00 04 64 72 63 34" +
                        "00 0a 66 6c 6f 61 74 5f  74 79 70 65 00 0a 05 05" +
                        "05 05 04 04 f6 f6 f6 f6  0e 08 08 08 08 04 04 0a" +
                        "00 0a 04 0a 00 0a 04 ff  03 4b 39 fe 99";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }


    @Test
    public void readCharsetTypeTest() {
        final ByteBuf byteBuf = initCharsetTypeByteBuf();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        final List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();

        final TableMapLogEvent.Column varchar = columns.get(0);
        Assert.assertEquals(mysql_type_varchar, MysqlFieldType.getMysqlFieldType(varchar.getType()));
        Assert.assertEquals(4000, varchar.getMeta());

        final TableMapLogEvent.Column char1000 = columns.get(1);
        Assert.assertEquals(mysql_type_string, MysqlFieldType.getMysqlFieldType(char1000.getType()));
//        Assert.assertEquals(1000, char1000.getMeta());

        final TableMapLogEvent.Column varbinary = columns.get(2);
        Assert.assertEquals(mysql_type_varchar, MysqlFieldType.getMysqlFieldType(varbinary.getType()));
        Assert.assertEquals(1800, varbinary.getMeta());

        final TableMapLogEvent.Column binary = columns.get(3);
        Assert.assertEquals(mysql_type_string, MysqlFieldType.getMysqlFieldType(binary.getType()));
//        Assert.assertEquals(200, binary.getMeta());

        final TableMapLogEvent.Column tinyblob = columns.get(4);
        Assert.assertEquals(mysql_type_blob, MysqlFieldType.getMysqlFieldType(tinyblob.getType()));
        Assert.assertEquals(1, tinyblob.getMeta());

        final TableMapLogEvent.Column mediumblob = columns.get(5);
        Assert.assertEquals(mysql_type_blob, MysqlFieldType.getMysqlFieldType(mediumblob.getType()));
        Assert.assertEquals(3, mediumblob.getMeta());

        final TableMapLogEvent.Column blob = columns.get(6);
        Assert.assertEquals(mysql_type_blob, MysqlFieldType.getMysqlFieldType(blob.getType()));
        Assert.assertEquals(2, blob.getMeta());

        final TableMapLogEvent.Column longblob = columns.get(7);
        Assert.assertEquals(mysql_type_blob, MysqlFieldType.getMysqlFieldType(longblob.getType()));
        Assert.assertEquals(4, longblob.getMeta());

        final TableMapLogEvent.Column tinytext = columns.get(8);
        Assert.assertEquals(mysql_type_blob, MysqlFieldType.getMysqlFieldType(tinytext.getType()));
        Assert.assertEquals(1, tinytext.getMeta());

        final TableMapLogEvent.Column mediumtext = columns.get(9);
        Assert.assertEquals(mysql_type_blob, MysqlFieldType.getMysqlFieldType(mediumtext.getType()));
        Assert.assertEquals(3, mediumtext.getMeta());

        final TableMapLogEvent.Column text = columns.get(10);
        Assert.assertEquals(mysql_type_blob, MysqlFieldType.getMysqlFieldType(text.getType()));
        Assert.assertEquals(2, text.getMeta());

        final TableMapLogEvent.Column longtext = columns.get(11);
        Assert.assertEquals(mysql_type_blob, MysqlFieldType.getMysqlFieldType(longtext.getType()));
        Assert.assertEquals(4, longtext.getMeta());

        final TableMapLogEvent.Column longtextWithoutCharset = columns.get(12);
        Assert.assertEquals(mysql_type_blob, MysqlFieldType.getMysqlFieldType(longtextWithoutCharset.getType()));
        Assert.assertEquals(4, longtextWithoutCharset.getMeta());
    }

    /**
     * sql:
     * CREATE TABLE `drc4`.`charset_type` (
     * `varchar4000` varchar(1000) CHARACTER SET utf8mb4,
     * `char1000` char(250) CHARACTER SET utf8mb4,
     * `varbinary1800` varbinary(1800),
     * `binary200` binary(200),
     * `tinyblob` tinyblob,
     * `mediumblob` mediumblob,
     * `blob` blob,
     * `longblob` longblob,
     * `tinytext` tinytext CHARACTER SET utf8mb4,
     * `mediumtext` mediumtext CHARACTER SET utf8mb4,
     * `text` text CHARACTER SET utf8mb4,
     * `longtext` longtext CHARACTER SET utf8mb4,
     * `longtextwithoutcharset` longtext
     * ) ENGINE=InnoDB;
     * <p>
     * mysql> desc `drc4`.`charset_type`;
     * +------------------------+-----------------+------+-----+---------+-------+
     * | Field                  | Type            | Null | Key | Default | Extra |
     * +------------------------+-----------------+------+-----+---------+-------+
     * | varchar4000            | varchar(1000)   | YES  |     | NULL    |       |
     * | char1000               | char(250)       | YES  |     | NULL    |       |
     * | varbinary1800          | varbinary(1800) | YES  |     | NULL    |       |
     * | binary200              | binary(200)     | YES  |     | NULL    |       |
     * | tinyblob               | tinyblob        | YES  |     | NULL    |       |
     * | mediumblob             | mediumblob      | YES  |     | NULL    |       |
     * | blob                   | blob            | YES  |     | NULL    |       |
     * | longblob               | longblob        | YES  |     | NULL    |       |
     * | tinytext               | tinytext        | YES  |     | NULL    |       |
     * | mediumtext             | mediumtext      | YES  |     | NULL    |       |
     * | text                   | text            | YES  |     | NULL    |       |
     * | longtext               | longtext        | YES  |     | NULL    |       |
     * | longtextwithoutcharset | longtext        | YES  |     | NULL    |       |
     * +------------------------+-----------------+------+-----+---------+-------+
     */
    private ByteBuf initCharsetTypeByteBuf() {
        String hexString =
                "20 84 a5 5d 13 01 00 00  00 55 00 00 00 1b 41 00" +
                        "00 00 00 5a 04 00 00 00  00 01 00 04 64 72 63 34" +
                        "00 0c 63 68 61 72 73 65  74 5f 74 79 70 65 00 0d" +
                        "0f fe 0f fe fc fc fc fc  fc fc fc fc fc 11 a0 0f" +
                        "ce e8 08 07 fe c8 01 03  02 04 01 03 02 04 04 ff" +
                        "1f 99 e0 cd dd";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    @Test
    public void readTimeTypeTest() {
        final ByteBuf byteBuf = initTimeTypeByteBuf();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        final List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();

        final TableMapLogEvent.Column date = columns.get(0);
        Assert.assertEquals(mysql_type_date, MysqlFieldType.getMysqlFieldType(date.getType()));
        Assert.assertEquals(0, date.getMeta());

        final TableMapLogEvent.Column time = columns.get(1);
        Assert.assertEquals(mysql_type_time2, MysqlFieldType.getMysqlFieldType(time.getType()));
        Assert.assertEquals(0, time.getMeta());

        final TableMapLogEvent.Column time6 = columns.get(2);
        Assert.assertEquals(mysql_type_time2, MysqlFieldType.getMysqlFieldType(time6.getType()));
        Assert.assertEquals(6, time6.getMeta());

        final TableMapLogEvent.Column datetime = columns.get(3);
        Assert.assertEquals(mysql_type_datetime2, MysqlFieldType.getMysqlFieldType(datetime.getType()));
        Assert.assertEquals(0, datetime.getMeta());

        final TableMapLogEvent.Column datetime6 = columns.get(4);
        Assert.assertEquals(mysql_type_datetime2, MysqlFieldType.getMysqlFieldType(datetime6.getType()));
        Assert.assertEquals(6, datetime6.getMeta());

        final TableMapLogEvent.Column timestamp = columns.get(5);
        Assert.assertEquals(mysql_type_timestamp2, MysqlFieldType.getMysqlFieldType(timestamp.getType()));
        Assert.assertEquals(0, timestamp.getMeta());

        final TableMapLogEvent.Column timestamp6 = columns.get(6);
        Assert.assertEquals(mysql_type_timestamp2, MysqlFieldType.getMysqlFieldType(timestamp6.getType()));
        Assert.assertEquals(6, timestamp6.getMeta());

        final TableMapLogEvent.Column year = columns.get(7);
        Assert.assertEquals(mysql_type_year, MysqlFieldType.getMysqlFieldType(year.getType()));
        Assert.assertEquals(0, year.getMeta());

        final TableMapLogEvent.Column year4 = columns.get(8);
        Assert.assertEquals(mysql_type_year, MysqlFieldType.getMysqlFieldType(year4.getType()));
        Assert.assertEquals(0, year4.getMeta());
    }


    /**
     * table description:
     * mysql> desc `drc4`.`time_type`;
     * +------------+--------------+------+-----+-------------------+-------+
     * | Field      | Type         | Null | Key | Default           | Extra |
     * +------------+--------------+------+-----+-------------------+-------+
     * | date       | date         | YES  |     | NULL              |       |
     * | time       | time         | YES  |     | NULL              |       |
     * | time6      | time(6)      | YES  |     | NULL              |       |
     * | datetime   | datetime     | YES  |     | CURRENT_TIMESTAMP |       |
     * | datetime6  | datetime(6)  | YES  |     | NULL              |       |
     * | timestamp  | timestamp    | NO   |     | CURRENT_TIMESTAMP |       |
     * | timestamp6 | timestamp(6) | YES  |     | NULL              |       |
     * | year       | year(4)      | YES  |     | NULL              |       |
     * | year4      | year(4)      | YES  |     | NULL              |       |
     * +------------+--------------+------+-----+-------------------+-------+
     * 9 rows in set (0.00 sec)
     */
    private ByteBuf initTimeTypeByteBuf() {
        String hexString =
                "73 c6 a6 5d 13 01 00 00  00 43 00 00 00 d2 78 00" +
                        "00 00 00 df 04 00 00 00  00 01 00 04 64 72 63 34" +
                        "00 09 74 69 6d 65 5f 74  79 70 65 00 09 0a 13 13" +
                        "12 12 11 11 0d 0d 06 00  06 00 06 00 06 df 01 41" +
                        "93 55 a4";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }


    @Test
    public void readEnumTypeTest() {
        final ByteBuf byteBuf = initEnumTypeByteBuf();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        final List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();

        final TableMapLogEvent.Column name = columns.get(0);
        Assert.assertEquals(mysql_type_varchar, MysqlFieldType.getMysqlFieldType(name.getType()));

        final TableMapLogEvent.Column size = columns.get(1);
        Assert.assertEquals(mysql_type_string, MysqlFieldType.getMysqlFieldType(size.getType()));
    }

    /**
     *  CREATE TABLE `table_json` (
     *   `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
     *   `data` json DEFAULT NULL COMMENT 'json',
     *   PRIMARY KEY (`id`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     */
    @Test
    public void testJsonDdl() {
        final ByteBuf byteBuf = initJsonTypeByteBuf();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        final List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();

        final TableMapLogEvent.Column column = columns.get(1);
        Assert.assertEquals(mysql_type_json, MysqlFieldType.getMysqlFieldType(column.getType()));
        Assert.assertEquals("data", column.getName());
    }

    /**
     * mysql> desc `drc4`.`enum`;
     * +-------+----------------------------------------------------+------+-----+---------+-------+
     * | Field | Type                                               | Null | Key | Default | Extra |
     * +-------+----------------------------------------------------+------+-----+---------+-------+
     * | name  | varchar(40)                                        | YES  |     | NULL    |       |
     * | size  | enum('x-small','small','medium','large','x-large') | YES  |     | NULL    |       |
     * +-------+----------------------------------------------------+------+-----+---------+-------+
     * 2 rows in set (0.00 sec)
     */
    private ByteBuf initEnumTypeByteBuf() {
        String hexString = "12 a0 b1 5d 13 01 00 00  00 34 00 00 00 71 5e 02" +
                "00 00 00 91 05 00 00 00  00 01 00 04 64 72 63 34" +
                "00 04 65 6e 75 6d 00 02  0f fe 04 28 00 f7 01 03" +
                "4e fe 39 96";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    private ByteBuf initJsonTypeByteBuf() {
        String hexString = "df 3f 8b 62   64   00 00 00 00   4a 00 00 00   4a 00 00 00   80 00" +
                "00 00 00 00 00 00 80 00  04 64 72 63 31 00 08 74" +
                "61 62 5f 6a 73 6f 6e 00  02 08 f5 01 00 02 02 69" +
                "64 04 64 61 74 61 03 03  00 00 01 00 00 03 01 01" +
                "02 69 64 00 00 00 00";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(400);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
