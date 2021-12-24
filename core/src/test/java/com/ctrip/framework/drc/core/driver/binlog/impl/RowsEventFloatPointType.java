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
public class RowsEventFloatPointType {

    @Test
    public void ReadFloatPointTest() {
        final ByteBuf byteBuf = initFloatPointByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockFloatPointColumns());

        final List<Object> values = writeRowsEvent.getBeforePresentRowsValues().get(0);
//        Assert.assertEquals(-123456789.12345679, values.get(0));
//        Assert.assertEquals(123456.1235, values.get(1));
//        Assert.assertEquals(-123456789.12345679, values.get(2));
//        Assert.assertEquals(123456.1235, values.get(3));
//        Assert.assertEquals(-123457000f, values.get(4));
//        Assert.assertEquals(123456.125, values.get(5));

        Assert.assertEquals(new BigDecimal("11111111112222222222333333333344444444445555555555666666666677777"), values.get(6));
        Assert.assertEquals(new BigDecimal("-11111111112222222222333333333344444444445555555555666666666677777"), values.get(7));
        Assert.assertEquals(new BigDecimal("0.111111111122222222223333333333"), values.get(8));
        Assert.assertEquals(new BigDecimal("-0.111111111122222222223333333333"), values.get(9));
        Assert.assertEquals(new BigDecimal("11111111112222222222333333333344444.111111111122222222223333333333"), values.get(10));
        Assert.assertEquals(new BigDecimal("-11111111112222222222333333333344444.111111111122222222223333333333"), values.get(11));

        Assert.assertEquals(new BigDecimal("99999999999999999999999999999999999999999999999999999999999999999"), values.get(12));
        Assert.assertEquals(new BigDecimal("0.000000000000000000000000000001"), values.get(13));
        Assert.assertEquals(new BigDecimal("-0.000000000000000000000000000001"), values.get(14));
        Assert.assertEquals(new BigDecimal("-99999999999999999999999999999999999999999999999999999999999999999"), values.get(15));

        Assert.assertEquals(new BigDecimal("-123456789"), values.get(16));
        Assert.assertEquals(new BigDecimal("123456.1235"), values.get(17));

        // sign and clear sign work
        final ByteBuf originByteBuf = initFloatPointByteBuf();
        for (int i = 0 ; i < originByteBuf.readableBytes(); i ++) {
            Assert.assertEquals(originByteBuf.getByte(i), byteBuf.getByte(i));
        }
    }

    /**
     * insert into `drc4`.`float_type` values (
     * -123456789.123456789, 123456.123456789,
     * -123456789.123456789, 123456.123456789,
     * -123456789.123456789, 123456.123456789,
     * 11111111112222222222333333333344444444445555555555666666666677777,
     * -11111111112222222222333333333344444444445555555555666666666677777,
     * 0.111111111122222222223333333333,
     * -0.111111111122222222223333333333,
     * 11111111112222222222333333333344444.111111111122222222223333333333,
     * -11111111112222222222333333333344444.111111111122222222223333333333,
     * 99999999999999999999999999999999999999999999999999999999999999999,
     * 0.000000000000000000000000000001,
     * -0.000000000000000000000000000001,
     * -99999999999999999999999999999999999999999999999999999999999999999,
     * -123456789.123456789, 123456.123456789
     * );
     * # at 64729
     * #191022  1:08:39 server id 1  end_log_pos 64831 CRC32 0x0fcaf6b0 Table_map: `drc4`.`float_type` mapped to number 1403
     * # at 64831
     * #191022  1:08:39 server id 1  end_log_pos 65153 CRC32 0x8b5eb69b Write_rows: table id 1403 flags: STMT_END_F
     * ### INSERT INTO `drc4`.`float_type`
     * ### SET
     * ###   @1=-123456789.12345679104  DOUBLE replicator=8 nullable=1 is_null=0
     * ###   @2=123456.12350000000151  DOUBLE replicator=8 nullable=1 is_null=0
     * ###   @3=-123456789.12345679104  DOUBLE replicator=8 nullable=1 is_null=0
     * ###   @4=123456.12350000000151  DOUBLE replicator=8 nullable=1 is_null=0
     * ###   @5=-1.23457e+08   FLOAT replicator=4 nullable=1 is_null=0
     * ###   @6=123456         FLOAT replicator=4 nullable=1 is_null=0
     * ###   @7=11111111112222222222333333333344444444445555555555666666666677777  DECIMAL(65,0) replicator=16640 nullable=1 is_null=0
     * ###   @8=-11111111112222222222333333333344444444445555555555666666666677777  DECIMAL(65,0) replicator=16640 nullable=1 is_null=0
     * ###   @9=0.111111111122222222223333333333  DECIMAL(30,30) replicator=7710 nullable=1 is_null=0
     * ###   @10=-0.111111111122222222223333333333  DECIMAL(30,30) replicator=7710 nullable=1 is_null=0
     * ###   @11=11111111112222222222333333333344444.111111111122222222223333333333  DECIMAL(65,30) replicator=16670 nullable=1 is_null=0
     * ###   @12=-11111111112222222222333333333344444.111111111122222222223333333333  DECIMAL(65,30) replicator=16670 nullable=1 is_null=0
     * ###   @13=99999999999999999999999999999999999999999999999999999999999999999  DECIMAL(65,0) replicator=16640 nullable=1 is_null=0
     * ###   @14=0.000000000000000000000000000001  DECIMAL(30,30) replicator=7710 nullable=1 is_null=0
     * ###   @15=-0.000000000000000000000000000001  DECIMAL(30,30) replicator=7710 nullable=1 is_null=0
     * ###   @16=-99999999999999999999999999999999999999999999999999999999999999999  DECIMAL(65,0) replicator=16640 nullable=1 is_null=0
     * ###   @17=-123456789  DECIMAL(10,0) replicator=2560 nullable=1 is_null=0
     * ###   @18=123456.1235  DECIMAL(10,4) replicator=2564 nullable=1 is_null=0
     */
    private ByteBuf initFloatPointByteBuf() {
        String hexString =
                "17 e6 ad 5d 1e 01 00 00  00 42 01 00 00 81 fe 00" +
                        "00 00 00 7b 05 00 00 00  00 01 00 02 00 12 ff ff" +
                        "ff 00 00 fc 75 6b 7e 54  34 6f 9d c1 d1 22 db f9" +
                        "01 24 fe 40 75 6b 7e 54  34 6f 9d c1 d1 22 db f9" +
                        "01 24 fe 40 a3 79 eb cc  10 20 f1 47 8b 06 9f 6b" + // 从第13个byte 8b开始是decimal_m_max_d_min_positive
                        "c8 0d 3e d7 8e 13 de 43  55 14 87 ce 1c 1a 8e a3" +
                        "63 21 1e cc ea 27 bc b2  11 74 f9 60 94 37 f2 c1" + // 从第10个byte 74开始是decimal_m_max_d_min_nagetive
                        "28 71 ec 21 bc aa eb 78  31 e3 e5 71 5c 9c de e1" +
                        "33 15 d8 43 4d ee 86 9f  6b c7 07 48 f6 8e 0d 4f" +
                        "cb d5 01 4d 79 60 94 38  f8 b7 09 71 f2 b0 34 2a" +
                        "fe b2 80 a9 8a c7 06 b0  60 0e 0d 40 89 95 13 de" +
                        "6e bc 06 9f 6b c7 07 48  f6 8e 0d 4f cb d5 01 4d" +
                        "7f 56 75 38 f9 4f 9f f1  f2 bf 76 6a ec 21 91 43" +
                        "f9 60 94 38 f8 b7 09 71  f2 b0 34 2a fe b2 e3 3b" +
                        "9a c9 ff 3b 9a c9 ff 3b  9a c9 ff 3b 9a c9 ff 3b" +
                        "9a c9 ff 3b 9a c9 ff 3b  9a c9 ff 80 00 00 00 00" +
                        "00 00 00 00 00 00 00 00  01 7f ff ff ff ff ff ff" +
                        "ff ff ff ff ff ff fe 1c  c4 65 36 00 c4 65 36 00" +
                        "c4 65 36 00 c4 65 36 00  c4 65 36 00 c4 65 36 00" +
                        "c4 65 36 00 7f f8 a4 32  ea 81 e2 40 04 d3 9b b6" +
                        "5e 8b";

//        String hexString = "f1 d1 ae 5d 1e 01 00 00  00 44 00 00 00 39 00 01" +
//                "00 00 00 7b 05 00 00 00  00 01 00 02 00 12 00 04" +
//                "00 fe 80 00 00 00 00 00  00 00 00 00 00 00 00 00" + // 从第3个byte 80开始
//                "00 01 05 f5 e1 00 00 00  00 00 00 00 00 00 00 00" +
//                "c9 4d d8 cc f1 d1 ae 5d  10 01 00 00 00 1f 00 00" +
//                "00 58 00 01 00 00 00 7f  1b 00 00 00 00 00 00 b8" +
//                "17 3b 19";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(500);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    /**
     * mysql> desc `drc4`.`float_type`;
     * +------------------------------+----------------+------+-----+---------+-------+
     * | Field                        | Type           | Null | Key | Default | Extra |
     * +------------------------------+----------------+------+-----+---------+-------+
     * | real                         | double         | YES  |     | NULL    |       |
     * | real10_4                     | double(10,4)   | YES  |     | NULL    |       |
     * | double                       | double         | YES  |     | NULL    |       |
     * | double10_4                   | double(10,4)   | YES  |     | NULL    |       |
     * | float                        | float          | YES  |     | NULL    |       |
     * | float10_4                    | float(10,4)    | YES  |     | NULL    |       |
     * | decimal_m_max_d_min_positive | decimal(65,0)  | YES  |     | NULL    |       |
     * | decimal_m_max_d_min_nagetive | decimal(65,0)  | YES  |     | NULL    |       |
     * | decimal_d_max_positive       | decimal(30,30) | YES  |     | NULL    |       |
     * | decimal_d_max_nagetive       | decimal(30,30) | YES  |     | NULL    |       |
     * | decimal_m_max_d_max_positive | decimal(65,30) | YES  |     | NULL    |       |
     * | decimal_m_max_d_max_nagetive | decimal(65,30) | YES  |     | NULL    |       |
     * | decimal_positive_max         | decimal(65,0)  | YES  |     | NULL    |       |
     * | decimal_positive_min         | decimal(30,30) | YES  |     | NULL    |       |
     * | decimal_nagetive_max         | decimal(30,30) | YES  |     | NULL    |       |
     * | decimal_nagetive_min         | decimal(65,0)  | YES  |     | NULL    |       |
     * | numeric                      | decimal(10,0)  | YES  |     | NULL    |       |
     * | numeric10_4                  | decimal(10,4)  | YES  |     | NULL    |       |
     * +------------------------------+----------------+------+-----+---------+-------+
     * 18 rows in set (0.01 sec)
     */
    private List<TableMapLogEvent.Column> mockFloatPointColumns() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column real = new TableMapLogEvent.Column("real", true, "double", null, null, null, null, null, null, "double", null, null, null);
        TableMapLogEvent.Column real10_4 = new TableMapLogEvent.Column("real10_4", true, "double", null, "10", "4", null, null, null, "double(10,4)", null, null, null);
        TableMapLogEvent.Column a_double = new TableMapLogEvent.Column("double", true, "double", null, null, null, null, null, null, "double", null, null, null);
        TableMapLogEvent.Column double10_4 = new TableMapLogEvent.Column("double10_4", true, "double", null, "10", "4", null, null, null, "double(10,4)", null, null, null);
        TableMapLogEvent.Column a_float = new TableMapLogEvent.Column("float", true, "float", null, null, null, null, null, null, "float", null, null, null);
        TableMapLogEvent.Column float10_4 = new TableMapLogEvent.Column("float10_4", true, "float", null, "10", "4", null, null, null, "float(10,4)", null, null, null);
        TableMapLogEvent.Column decimal_m_max_d_min_positive = new TableMapLogEvent.Column("decimal_m_max_d_min_positive", true, "decimal", null, "65", "0", null, null, null, "decimal(65,0)", null, null, null);
        TableMapLogEvent.Column decimal_m_max_d_min_nagetive = new TableMapLogEvent.Column("decimal_m_max_d_min_nagetive", true, "decimal", null, "65", "0", null, null, null, "decimal(65,0)", null, null, null);
        TableMapLogEvent.Column decimal_d_max_positive = new TableMapLogEvent.Column("decimal_d_max_positive", true, "decimal", null, "30", "30", null, null, null, "decimal(30,30)", null, null, null);
        TableMapLogEvent.Column decimal_d_max_nagetive = new TableMapLogEvent.Column("decimal_d_max_nagetive", true, "decimal", null, "30", "30", null, null, null, "decimal(30,30)", null, null, null);
        TableMapLogEvent.Column decimal_m_max_d_max_positive = new TableMapLogEvent.Column("decimal_m_max_d_max_positive", true, "decimal", null, "65", "30", null, null, null, "decimal(65,30)", null, null, null);
        TableMapLogEvent.Column decimal_m_max_d_max_nagetive = new TableMapLogEvent.Column("decimal_m_max_d_max_nagetive", true, "decimal", null, "65", "30", null, null, null, "decimal(65,30)", null, null, null);
        TableMapLogEvent.Column decimal_positive_max = new TableMapLogEvent.Column("decimal_positive_max", true, "decimal", null, "65", "0", null, null, null, "decimal(65,0)", null, null, null);
        TableMapLogEvent.Column decimal_positive_min = new TableMapLogEvent.Column("decimal_positive_min", true, "decimal", null, "30", "30", null, null, null, "decimal(30,30)", null, null, null);
        TableMapLogEvent.Column decimal_nagetive_max = new TableMapLogEvent.Column("decimal_nagetive_max", true, "decimal", null, "30", "30", null, null, null, "decimal(30,30)", null, null, null);
        TableMapLogEvent.Column decimal_nagetive_min = new TableMapLogEvent.Column("decimal_nagetive_min", true, "decimal", null, "65", "0", null, null, null, "decimal(65,0)", null, null, null);
        TableMapLogEvent.Column numeric = new TableMapLogEvent.Column("numeric", true, "decimal", null, "10", "0", null, null, null, "decimal(10,0)", null, null, null);
        TableMapLogEvent.Column numeric10_4 = new TableMapLogEvent.Column("numeric10_4", true, "decimal", null, "10", "4", null, null, null, "decimal(10,4)", null, null, null);

        columns.add(real);
        columns.add(real10_4);
        columns.add(a_double);
        columns.add(double10_4);
        columns.add(a_float);
        columns.add(float10_4);
        columns.add(decimal_m_max_d_min_positive);
        columns.add(decimal_m_max_d_min_nagetive);
        columns.add(decimal_d_max_positive);
        columns.add(decimal_d_max_nagetive);
        columns.add(decimal_m_max_d_max_positive);
        columns.add(decimal_m_max_d_max_nagetive);
        columns.add(decimal_positive_max);
        columns.add(decimal_positive_min);
        columns.add(decimal_nagetive_max);
        columns.add(decimal_nagetive_min);
        columns.add(numeric);
        columns.add(numeric10_4);
        return columns;
    }

    @Test
    public void readDecimalScaleTest() {
        final ByteBuf byteBuf = initDecimalScaleByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockDecimalScaleTable());

        final List<Object> values = writeRowsEvent.getBeforePresentRowsValues().get(0);
        Assert.assertEquals(new BigDecimal("12345.00"), values.get(0));
        Assert.assertEquals(new BigDecimal("12345.00"), values.get(1));
        Assert.assertEquals(new BigDecimal("12345.000"), values.get(2));
        Assert.assertEquals(new BigDecimal("12345.0000"), values.get(3));
        Assert.assertEquals(new BigDecimal("12345.00000"), values.get(4));
    }

    /*
    mysql> insert into `drc4`.`decimal_scale` values (12345.00000, 12345.00000, 12345.00000, 12345.00000, 12345.00000);
    Query OK, 1 row affected (0.01 sec)

    mysql> select * from decimal_scale;
    +----------+----------+-----------+------------+-------------+
    | column1  | column2  | column3   | column4    | column5     |
    +----------+----------+-----------+------------+-------------+
    | 12345.00 | 12345.00 | 12345.000 | 12345.0000 | 12345.00000 |
    +----------+----------+-----------+------------+-------------+
    1 row in set (0.00 sec)
     */
    private ByteBuf initDecimalScaleByteBuf() {
        String hexString =
                "59 e7 7c 5e 1e 01 00 00  00 99 00 00 00 b6 05 00" +
                        "00 00 00 7c 00 00 00 00  00 01 00 02 00 05 ff e0" +
                        "80 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00" +
                        "00 00 00 00 00 00 00 00  00 00 30 39 00 80 00 00" +
                        "00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00" +
                        "00 00 00 00 00 00 00 30  39 00 80 00 00 00 00 00" +
                        "00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00" +
                        "00 00 00 00 30 39 00 00  80 00 00 00 00 00 00 00" +
                        "00 00 30 39 00 00 80 00  00 00 00 00 00 00 00 00" +
                        "30 39 00 00 00 c2 d9 4a  ab";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(500);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    /**
     CREATE TABLE `decimal_scale` (
     `column1` decimal(65,2) DEFAULT NULL,
     `column2` decimal(65,2) DEFAULT NULL,
     `column3` decimal(65,3) DEFAULT NULL,
     `column4` decimal(30,4) DEFAULT NULL,
     `column5` decimal(30,5) DEFAULT NULL
     ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
     */
    private List<TableMapLogEvent.Column> mockDecimalScaleTable() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("column1", true, "decimal", null, "65", "2", null, null, null, "decimal(65,2)", null, null, null);
        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("column2", true, "decimal", null, "65", "2", null, null, null, "decimal(65,2)", null, null, null);
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("column3", true, "decimal", null, "65", "3", null, null, null, "decimal(65,3)", null, null, null);
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("column4", true, "decimal", null, "30", "4", null, null, null, "decimal(30,4)", null, null, null);
        TableMapLogEvent.Column column5 = new TableMapLogEvent.Column("column5", true, "decimal", null, "30", "5", null, null, null, "decimal(30,5)", null, null, null);

        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columns.add(column4);
        columns.add(column5);
        return columns;
    }
}
