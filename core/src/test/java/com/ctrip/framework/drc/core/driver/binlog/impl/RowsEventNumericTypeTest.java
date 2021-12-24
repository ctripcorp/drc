package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by @author zhuYongMing on 2019/10/14.
 */
public class RowsEventNumericTypeTest {

    /**
     * test for decode signed numeric type value max boundary rowsEvent
     */
    @Test
    public void readSignedNumericTypeValueMaxBoundaryTest() {
        final ByteBuf byteBuf = initSignedNumericTypeValueMaxBoundaryByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockSignedTableColumns());

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforePresentRowsValues.size());

        final List<Object> beforePresentRows1Values = beforePresentRowsValues.get(0);
        Assert.assertEquals(14, beforePresentRows1Values.size());
        Assert.assertEquals((short) 255, beforePresentRows1Values.get(0));
        Assert.assertEquals(65535, beforePresentRows1Values.get(1));
        Assert.assertEquals(16777215, beforePresentRows1Values.get(2));
        Assert.assertEquals(4294967295L, beforePresentRows1Values.get(3));
        Assert.assertEquals(1099511627775L, beforePresentRows1Values.get(4));
        Assert.assertEquals(281474976710655L, beforePresentRows1Values.get(5));
        Assert.assertEquals(72057594037927935L, beforePresentRows1Values.get(6));
        Assert.assertEquals(new BigDecimal("18446744073709551615"), beforePresentRows1Values.get(7));

        Assert.assertEquals((byte) 127, beforePresentRows1Values.get(8));
        Assert.assertEquals((short) 32767, beforePresentRows1Values.get(9));
        Assert.assertEquals(8388607, beforePresentRows1Values.get(10));
        Assert.assertEquals(2147483647, beforePresentRows1Values.get(11));
        Assert.assertEquals(2147483647, beforePresentRows1Values.get(12));
        Assert.assertEquals(9223372036854775807L, beforePresentRows1Values.get(13));
    }

    /**
     * insert into `drc4`.`multi_type_number` values (255, 65535, 16777215, 4294967295, 1099511627775, 281474976710655, 72057594037927935, 18446744073709551615,
     * 127, 32767, 8388607, 2147483647, 2147483647, 9223372036854775807
     * );
     * <p>
     * # at 22604
     * #191012 14:42:14 server id 1  end_log_pos 22700 CRC32 0x89bab06c        Write_rows: table id 245 flags: STMT_END_F
     * ### INSERT INTO `drc4`.`multi_type_number`
     * ### SET
     * ###   @1=b'11111111'  BIT(8) replicator=256 nullable=1 is_null=0
     * ###   @2=b'1111111111111111'  BIT(16) replicator=512 nullable=1 is_null=0
     * ###   @3=b'111111111111111111111111'  BIT(24) replicator=768 nullable=1 is_null=0
     * ###   @4=b'11111111111111111111111111111111'  BIT(32) replicator=1024 nullable=1 is_null=0
     * ###   @5=b'1111111111111111111111111111111111111111'  BIT(40) replicator=1280 nullable=1 is_null=0
     * ###   @6=b'111111111111111111111111111111111111111111111111'  BIT(48) replicator=1536 nullable=1 is_null=0
     * ###   @7=b'11111111111111111111111111111111111111111111111111111111'  BIT(56) replicator=1792 nullable=1 is_null=0
     * ###   @8=b'1111111111111111111111111111111111111111111111111111111111111111'  BIT(64) replicator=2048 nullable=1 is_null=0
     * ###   @9=127  TINYINT replicator=0 nullable=1 is_null=0
     * ###   @10=32767  SHORTINT replicator=0 nullable=1 is_null=0
     * ###   @11=8388607  MEDIUMINT replicator=0 nullable=1 is_null=0
     * ###   @12=2147483647  INT replicator=0 nullable=1 is_null=0
     * ###   @13=2147483647  INT replicator=0 nullable=1 is_null=0
     * ###   @14=9223372036854775807  LONGINT replicator=0 nullable=1 is_null=0
     */
    private ByteBuf initSignedNumericTypeValueMaxBoundaryByteBuf() {
        String hexString =
                "c6 75 a1 5d 1e 01 00 00  00 60 00 00 00 ac 58 00" +
                        "00 00 00 f5 00 00 00 00  00 01 00 02 00 0e ff ff" +
                        "00 c0 ff ff ff ff ff ff  ff ff ff ff ff ff ff ff" +
                        "ff ff ff ff ff ff ff ff  ff ff ff ff ff ff ff ff" +
                        "ff ff ff ff ff ff 7f ff  7f ff ff 7f ff ff ff 7f" +
                        "ff ff ff 7f ff ff ff ff  ff ff ff 7f 6c b0 ba 89";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    /**
     * mysql> desc `drc4`.`multi_type_number`;
     * +-----------+---------------+------+-----+---------+-------+
     * | Field     | Type          | Null | Key | Default | Extra |
     * +-----------+---------------+------+-----+---------+-------+
     * | bit1      | bit(8)        | YES  |     | NULL    |       |
     * | bit2      | bit(16)       | YES  |     | NULL    |       |
     * | bit3      | bit(24)       | YES  |     | NULL    |       |
     * | bit4      | bit(32)       | YES  |     | NULL    |       |
     * | bit5      | bit(40)       | YES  |     | NULL    |       |
     * | bit6      | bit(48)       | YES  |     | NULL    |       |
     * | bit7      | bit(56)       | YES  |     | NULL    |       |
     * | bit8      | bit(64)       | YES  |     | NULL    |       |
     * | tinyint   | tinyint(5)    | YES  |     | NULL    |       |
     * | smallint  | smallint(10)  | YES  |     | NULL    |       |
     * | mediumint | mediumint(15) | YES  |     | NULL    |       |
     * | int       | int(20)       | YES  |     | NULL    |       |
     * | integer   | int(20)       | YES  |     | NULL    |       |
     * | bigint    | bigint(100)   | YES  |     | NULL    |       |
     * +-----------+---------------+------+-----+---------+-------+
     * 14 rows in set (0.00 sec)
     */
    private List<TableMapLogEvent.Column> mockSignedTableColumns() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column bit1 = new TableMapLogEvent.Column("bit1", true, "bit", null, "8", null, null, null, null, "bit(8)", null, null, null);
        TableMapLogEvent.Column bit2 = new TableMapLogEvent.Column("bit2", true, "bit", null, "16", null, null, null, null, "bit(16)", null, null, null);
        TableMapLogEvent.Column bit3 = new TableMapLogEvent.Column("bit3", true, "bit", null, "24", null, null, null, null, "bit(24)", null, null, null);
        TableMapLogEvent.Column bit4 = new TableMapLogEvent.Column("bit4", true, "bit", null, "32", null, null, null, null, "bit(32)", null, null, null);
        TableMapLogEvent.Column bit5 = new TableMapLogEvent.Column("bit5", true, "bit", null, "40", null, null, null, null, "bit(40)", null, null, null);
        TableMapLogEvent.Column bit6 = new TableMapLogEvent.Column("bit6", true, "bit", null, "48", null, null, null, null, "bit(48)", null, null, null);
        TableMapLogEvent.Column bit7 = new TableMapLogEvent.Column("bit7", true, "bit", null, "56", null, null, null, null, "bit(56)", null, null, null);
        TableMapLogEvent.Column bit8 = new TableMapLogEvent.Column("bit8", true, "bit", null, "64", null, null, null, null, "bit(64)", null, null, null);

        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("tinyint", true, "tinyint", null, "5", null, null, null, null, "tinyint(5)", null, null, null);
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("smallint", true, "smallint", null, "10", null, null, null, null, "smallint(10)", null, null, null);
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("mediumint", true, "mediumint", null, "15", null, null, null, null, "mediumint(10)", null, null, null);
        TableMapLogEvent.Column column5 = new TableMapLogEvent.Column("int", true, "int", null, "20", null, null, null, null, "int(20)", null, null, null);
        TableMapLogEvent.Column column6 = new TableMapLogEvent.Column("integer", true, "int", null, "20", null, null, null, null, "int(20)", null, null, null);
        TableMapLogEvent.Column column7 = new TableMapLogEvent.Column("bigint", true, "bigint", null, "100", null, null, null, null, "bigint(100)", null, null, null);
        columns.add(bit1);
        columns.add(bit2);
        columns.add(bit3);
        columns.add(bit4);
        columns.add(bit5);
        columns.add(bit6);
        columns.add(bit7);
        columns.add(bit8);

        columns.add(column2);
        columns.add(column3);
        columns.add(column4);
        columns.add(column5);
        columns.add(column6);
        columns.add(column7);
        return columns;
    }

    /**
     * test for decode signed numeric type value min boundary rowsEvent
     */
    @Test
    public void readSignedNumericTypeValueMinBoundaryTest() {
        final ByteBuf byteBuf = initSignedNumericTypeValueMinBoundaryByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockSignedTableColumns());

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforePresentRowsValues.size());

        final List<Object> beforePresentRows1Values = beforePresentRowsValues.get(0);
        Assert.assertEquals(14, beforePresentRows1Values.size());
        Assert.assertEquals((short) 0, beforePresentRows1Values.get(0));
        Assert.assertEquals(0, beforePresentRows1Values.get(1));
        Assert.assertEquals(0, beforePresentRows1Values.get(2));
        Assert.assertEquals(0L, beforePresentRows1Values.get(3));
        Assert.assertEquals(0L, beforePresentRows1Values.get(4));
        Assert.assertEquals(0L, beforePresentRows1Values.get(5));
        Assert.assertEquals(0L, beforePresentRows1Values.get(6));
        Assert.assertEquals(new BigDecimal("0"), beforePresentRows1Values.get(7));

        Assert.assertEquals((byte) -128, beforePresentRows1Values.get(8));
        Assert.assertEquals((short) -32768, beforePresentRows1Values.get(9));
        Assert.assertEquals(-8388608, beforePresentRows1Values.get(10));
        Assert.assertEquals(-2147483648, beforePresentRows1Values.get(11));
        Assert.assertEquals(-2147483648, beforePresentRows1Values.get(12));
        Assert.assertEquals(-9223372036854775808L, beforePresentRows1Values.get(13));
    }

    /**
     * # insert into `drc4`.`multi_type_number` values (0, 0, 0, 0, 0, 0, 0, 0,
     * #   -128, -32768, -8388608, -2147483648, -2147483648, -9223372036854775808
     * # )
     * <p>
     * # at 23128
     * #191012 15:48:03 server id 1  end_log_pos 23224 CRC32 0xdf48d105        Write_rows: table id 245 flags: STMT_END_F
     * ### INSERT INTO `drc4`.`multi_type_number`
     * ### SET
     * ###   @1=b'00000000'  BIT(8) replicator=256 nullable=1 is_null=0
     * ###   @2=b'0000000000000000'  BIT(16) replicator=512 nullable=1 is_null=0
     * ###   @3=b'000000000000000000000000' /* BIT(24) replicator=768 nullable=1 is_null=0
     * ###   @4=b'00000000000000000000000000000000' /* BIT(32) replicator=1024 nullable=1 is_null=0
     * ###   @5=b'0000000000000000000000000000000000000000' /* BIT(40) replicator=1280 nullable=1 is_null=0
     * ###   @6=b'000000000000000000000000000000000000000000000000' /* BIT(48) replicator=1536 nullable=1 is_null=0
     * ###   @7=b'00000000000000000000000000000000000000000000000000000000' /* BIT(56) replicator=1792 nullable=1 is_null=0
     * ###   @8=b'0000000000000000000000000000000000000000000000000000000000000000' /* BIT(64) replicator=2048 nullable=1 is_null=0
     * ###   @9=-128 (128)  TINYINT replicator=0 nullable=1 is_null=0
     * ###   @10=-32768 (32768)  SHORTINT replicator=0 nullable=1 is_null=0
     * ###   @11=-8388608 (8388608)  MEDIUMINT replicator=0 nullable=1 is_null=0
     * ###   @12=-2147483648 (2147483648)  INT replicator=0 nullable=1 is_null=0
     * ###   @13=-2147483648 (2147483648)  INT replicator=0 nullable=1 is_null=0
     * ###   @14=-9223372036854775808 (9223372036854775808)  LONGINT replicator=0 nullable=1 is_null=0
     */
    private ByteBuf initSignedNumericTypeValueMinBoundaryByteBuf() {
        String hexString =
                "33 85 a1 5d 1e 01 00 00  00 60 00 00 00 b8 5a 00" +
                        "00 00 00 f5 00 00 00 00  00 01 00 02 00 0e ff ff" +
                        "00 c0 00 00 00 00 00 00  00 00 00 00 00 00 00 00" +
                        "00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00" +
                        "00 00 00 00 00 00 80 00  80 00 00 80 00 00 00 80" + //第七个开始是tinyint
                        "00 00 00 80 00 00 00 00  00 00 00 80 05 d1 48 df";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    /**
     * test for decode unsigned numeric type value max boundary rowsEvent
     */
    @Test
    public void readUnsignedNumericTypeValueMaxBoundaryTest() {
        final ByteBuf byteBuf = initUnsignedNumericTypeValueMaxBoundaryByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockUnSignedTableColumns());

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforePresentRowsValues.size());

        final List<Object> beforePresentRows1Values = beforePresentRowsValues.get(0);
        Assert.assertEquals(14, beforePresentRows1Values.size());
        Assert.assertEquals((short) 0, beforePresentRows1Values.get(0));
        Assert.assertEquals(0, beforePresentRows1Values.get(1));
        Assert.assertEquals(0, beforePresentRows1Values.get(2));
        Assert.assertEquals(0L, beforePresentRows1Values.get(3));
        Assert.assertEquals(0L, beforePresentRows1Values.get(4));
        Assert.assertEquals(0L, beforePresentRows1Values.get(5));
        Assert.assertEquals(72057594037927935L, beforePresentRows1Values.get(6));
        Assert.assertEquals(new BigDecimal("18446744073709551615"), beforePresentRows1Values.get(7));

        Assert.assertEquals((short) 255, beforePresentRows1Values.get(8));
        Assert.assertEquals(65535, beforePresentRows1Values.get(9));
        Assert.assertEquals(16777215, beforePresentRows1Values.get(10));
        Assert.assertEquals(4294967295L, beforePresentRows1Values.get(11));
        Assert.assertEquals(4294967295L, beforePresentRows1Values.get(12));
        Assert.assertEquals(new BigDecimal("18446744073709551615"), beforePresentRows1Values.get(13));
    }

    /**
     * insert into `drc4`.`multi_type_number_unsigned` values (0, 0, 0, 0, 0, 0, 72057594037927935, 18446744073709551615,
     * 255, 65535, 16777215, 4294967295, 4294967295, 18446744073709551615
     * )
     * <p>
     * # at 24866
     * #191012 16:53:23 server id 1  end_log_pos 24962 CRC32 0x26e1c1a5        Write_rows: table id 246 flags: STMT_END_F
     * ### INSERT INTO `drc4`.`multi_type_number_unsigned`
     * ### SET
     * ###   @1=b'00000000'  BIT(8) replicator=256 nullable=1 is_null=0
     * ###   @2=b'0000000000000000'  BIT(16) replicator=512 nullable=1 is_null=0
     * ###   @3=b'000000000000000000000000' /* BIT(24) replicator=768 nullable=1 is_null=0
     * ###   @4=b'00000000000000000000000000000000' /* BIT(32) replicator=1024 nullable=1 is_null=0
     * ###   @5=b'0000000000000000000000000000000000000000' /* BIT(40) replicator=1280 nullable=1 is_null=0
     * ###   @6=b'000000000000000000000000000000000000000000000000' /* BIT(48) replicator=1536 nullable=1 is_null=0
     * ###   @7=b'11111111111111111111111111111111111111111111111111111111' /* BIT(56) replicator=1792 nullable=1 is_null=0
     * ###   @8=b'1111111111111111111111111111111111111111111111111111111111111111' /* BIT(64) replicator=2048 nullable=1 is_null=0
     * ###   @9=-1 (255)  TINYINT replicator=0 nullable=1 is_null=0
     * ###   @10=-1 (65535)  SHORTINT replicator=0 nullable=1 is_null=0
     * ###   @11=-1 (16777215)  MEDIUMINT replicator=0 nullable=1 is_null=0
     * ###   @12=-1 (4294967295)  INT replicator=0 nullable=1 is_null=0
     * ###   @13=-1 (4294967295)  INT replicator=0 nullable=1 is_null=0
     * ###   @14=-1 (18446744073709551615)  LONGINT replicator=0 nullable=1 is_null=0
     */
    private ByteBuf initUnsignedNumericTypeValueMaxBoundaryByteBuf() {
        String hexString =
                "83 94 a1 5d 1e 01 00 00  00 60 00 00 00 82 61 00" +
                        "00 00 00 f6 00 00 00 00  00 01 00 02 00 0e ff ff" +
                        "00 c0 00 00 00 00 00 00  00 00 00 00 00 00 00 00" +
                        "00 00 00 00 00 00 00 ff  ff ff ff ff ff ff ff ff" +
                        "ff ff ff ff ff ff ff ff  ff ff ff ff ff ff ff ff" + //第七个开始是tinyint
                        "ff ff ff ff ff ff ff ff  ff ff ff ff a5 c1 e1 26";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    /**
     * mysql> desc `drc4`.`multi_type_number_unsigned`;
     * +-----------+------------------------+------+-----+---------+-------+
     * | Field     | Type                   | Null | Key | Default | Extra |
     * +-----------+------------------------+------+-----+---------+-------+
     * | bit1      | bit(8)                 | YES  |     | NULL    |       |
     * | bit2      | bit(16)                | YES  |     | NULL    |       |
     * | bit3      | bit(24)                | YES  |     | NULL    |       |
     * | bit4      | bit(32)                | YES  |     | NULL    |       |
     * | bit5      | bit(40)                | YES  |     | NULL    |       |
     * | bit6      | bit(48)                | YES  |     | NULL    |       |
     * | bit7      | bit(56)                | YES  |     | NULL    |       |
     * | bit8      | bit(64)                | YES  |     | NULL    |       |
     * | tinyint   | tinyint(5) unsigned    | YES  |     | NULL    |       |
     * | smallint  | smallint(10) unsigned  | YES  |     | NULL    |       |
     * | mediumint | mediumint(15) unsigned | YES  |     | NULL    |       |
     * | int       | int(20) unsigned       | YES  |     | NULL    |       |
     * | integer   | int(20) unsigned       | YES  |     | NULL    |       |
     * | bigint    | bigint(100) unsigned   | YES  |     | NULL    |       |
     * +-----------+------------------------+------+-----+---------+-------+
     * 14 rows in set (0.01 sec)
     */
    private List<TableMapLogEvent.Column> mockUnSignedTableColumns() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column bit1 = new TableMapLogEvent.Column("bit1", true, "bit", null, "8", null, null, null, null, "bit(8)", null, null, "NULL");
        TableMapLogEvent.Column bit2 = new TableMapLogEvent.Column("bit2", true, "bit", null, "16", null, null, null, null, "bit(16)", null, null, "NULL");
        TableMapLogEvent.Column bit3 = new TableMapLogEvent.Column("bit3", true, "bit", null, "24", null, null, null, null, "bit(24)", null, null, "NULL");
        TableMapLogEvent.Column bit4 = new TableMapLogEvent.Column("bit4", true, "bit", null, "32", null, null, null, null, "bit(32)", null, null, "NULL");
        TableMapLogEvent.Column bit5 = new TableMapLogEvent.Column("bit5", true, "bit", null, "40", null, null, null, null, "bit(40)", null, null, "NULL");
        TableMapLogEvent.Column bit6 = new TableMapLogEvent.Column("bit6", true, "bit", null, "48", null, null, null, null, "bit(48)", null, null, "NULL");
        TableMapLogEvent.Column bit7 = new TableMapLogEvent.Column("bit7", true, "bit", null, "56", null, null, null, null, "bit(56)", null, null, "NULL");
        TableMapLogEvent.Column bit8 = new TableMapLogEvent.Column("bit8", true, "bit", null, "64", null, null, null, null, "bit(64)", null, null, "NULL");

        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("tinyint", true, "tinyint", null, "5", null, null, null, null, "tinyint(5) unsigned", null, null, "NULL");
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("smallint", true, "smallint", null, "10", null, null, null, null, "smallint(10) unsigned", null, null, "NULL");
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("mediumint", true, "mediumint", null, "15", null, null, null, null, "mediumint(10) unsigned", null, null, "NULL");
        TableMapLogEvent.Column column5 = new TableMapLogEvent.Column("int", true, "int", null, "20", null, null, null, null, "int(20) unsigned", null, null, "NULL");
        TableMapLogEvent.Column column6 = new TableMapLogEvent.Column("integer", true, "int", null, "20", null, null, null, null, "int(20) unsigned", null, null, "NULL");
        TableMapLogEvent.Column column7 = new TableMapLogEvent.Column("bigint", true, "bigint", null, "100", null, null, null, null, "bigint(100) unsigned", null, null, "NULL");
        columns.add(bit1);
        columns.add(bit2);
        columns.add(bit3);
        columns.add(bit4);
        columns.add(bit5);
        columns.add(bit6);
        columns.add(bit7);
        columns.add(bit8);

        columns.add(column2);
        columns.add(column3);
        columns.add(column4);
        columns.add(column5);
        columns.add(column6);
        columns.add(column7);
        return columns;
    }

    /**
     * 取值介于[signed max, unsigned max]之间
     */
    @Test
    public void readUnsignedNumericTypeValueTest() {
        final ByteBuf byteBuf = initUnsignedNumericTypeValueLossOfPrecisionByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockUnSignedTableColumns());

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforePresentRowsValues.size());

        final List<Object> beforePresentRows1Values = beforePresentRowsValues.get(0);
        Assert.assertEquals(14, beforePresentRows1Values.size());
        Assert.assertEquals((short) 200, beforePresentRows1Values.get(0));
        Assert.assertEquals(40000, beforePresentRows1Values.get(1));
        Assert.assertEquals(10000000, beforePresentRows1Values.get(2));
        Assert.assertEquals(3000000000L, beforePresentRows1Values.get(3));
        Assert.assertEquals(600000000000L, beforePresentRows1Values.get(4));
        Assert.assertEquals(200000000000000L, beforePresentRows1Values.get(5));
        Assert.assertEquals(40000000000000000L, beforePresentRows1Values.get(6));
        Assert.assertEquals(new BigDecimal("10000000000000000000"), beforePresentRows1Values.get(7));

        Assert.assertEquals((short) 200, beforePresentRows1Values.get(8));
        Assert.assertEquals(40000, beforePresentRows1Values.get(9));
        Assert.assertEquals(10000000, beforePresentRows1Values.get(10));
        Assert.assertEquals(3000000000L, beforePresentRows1Values.get(11));
        Assert.assertEquals(3000000000L, beforePresentRows1Values.get(12));
        Assert.assertEquals(new BigDecimal("10000000000000000000"), beforePresentRows1Values.get(13));
    }


    /**
     * insert into `drc4`.`multi_type_number_unsigned` values (200, 40000, 10000000, 3000000000, 600000000000, 200000000000000, 40000000000000000, 10000000000000000000,
     * 200, 40000, 10000000, 3000000000, 3000000000, 10000000000000000000
     * );
     * <p>
     * # at 273616
     * #191014 15:48:54 server id 1  end_log_pos 273712 CRC32 0x8af71735       Write_rows: table id 246 flags: STMT_END_F
     * ### INSERT INTO `drc4`.`multi_type_number_unsigned`
     * ### SET
     * ###   @1=b'11001000'  BIT(8) replicator=256 nullable=1 is_null=0
     * ###   @2=b'1001110001000000'  BIT(16) replicator=512 nullable=1 is_null=0
     * ###   @3=b'100110001001011010000000'  BIT(24) replicator=768 nullable=1 is_null=0
     * ###   @4=b'10110010110100000101111000000000'  BIT(32) replicator=1024 nullable=1 is_null=0
     * ###   @5=b'1000101110110010110010010111000000000000'  BIT(40) replicator=1280 nullable=1 is_null=0
     * ###   @6=b'101101011110011000100000111101001000000000000000'  BIT(48) replicator=1536 nullable=1 is_null=0
     * ###   @7=b'10001110000110111100100110111111000001000000000000000000'  BIT(56) replicator=1792 nullable=1 is_null=0
     * ###   @8=b'1000101011000111001000110000010010001001111010000000000000000000'  BIT(64) replicator=2048 nullable=1 is_null=0
     * ###   @9=-56 (200)  TINYINT replicator=0 nullable=1 is_null=0
     * ###   @10=-25536 (40000)  SHORTINT replicator=0 nullable=1 is_null=0
     * ###   @11=-6777216 (10000000)  MEDIUMINT replicator=0 nullable=1 is_null=0
     * ###   @12=-1294967296 (3000000000)  INT replicator=0 nullable=1 is_null=0
     * ###   @13=-1294967296 (3000000000)  INT replicator=0 nullable=1 is_null=0
     * ###   @14=-8446744073709551616 (10000000000000000000)  LONGINT replicator=0 nullable=1 is_null=0
     */
    private ByteBuf initUnsignedNumericTypeValueLossOfPrecisionByteBuf() {
        String hexString =
                "66 28 a4 5d 1e 01 00 00  00 60 00 00 00 30 2d 04" +
                        "00 00 00 f6 00 00 00 00  00 01 00 02 00 0e ff ff" +
                        "00 c0 c8 9c 40 98 96 80  b2 d0 5e 00 8b b2 c9 70" +
                        "00 b5 e6 20 f4 80 00 8e  1b c9 bf 04 00 00 8a c7" +
                        "23 04 89 e8 00 00 c8 40  9c 80 96 98 00 5e d0 b2" + //第七个开始是tinyint
                        "00 5e d0 b2 00 00 e8 89  04 23 c7 8a 35 17 f7 8a";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
