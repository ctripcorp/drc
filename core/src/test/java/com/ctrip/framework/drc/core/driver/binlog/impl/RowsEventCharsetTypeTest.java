package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by @author zhuYongMing on 2019/10/14.
 */
public class RowsEventCharsetTypeTest {

    @Test
    public void readCharsetTypeTest() {
        final ByteBuf byteBuf = initCharsetTypeByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockCharsetTypeTable());

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforePresentRowsValues.size());

        final List<Object> beforePresentRows1Values = beforePresentRowsValues.get(0);
        Assert.assertEquals(13, beforePresentRows1Values.size());

        Assert.assertEquals("嘿嘿嘿varchar4000", beforePresentRows1Values.get(0));
        Assert.assertEquals("嘿嘿嘿char1000", beforePresentRows1Values.get(1));
        Assert.assertEquals("varbinary1800", new String((byte[]) beforePresentRows1Values.get(2)));
        Assert.assertEquals(assertBinary, new String((byte[]) beforePresentRows1Values.get(3)));
        Assert.assertEquals(assertBinary, new String((byte[]) beforePresentRows1Values.get(4)));
        Assert.assertEquals(assertBinary, new String((byte[]) beforePresentRows1Values.get(5)));
        Assert.assertEquals(assertBinary, new String((byte[]) beforePresentRows1Values.get(6)));
        Assert.assertEquals(assertBinary, new String((byte[]) beforePresentRows1Values.get(7)));
        Assert.assertEquals("嘿嘿嘿tinytext", new String((byte[]) beforePresentRows1Values.get(8)));
        Assert.assertEquals("嘿嘿嘿mediumtext", new String((byte[]) beforePresentRows1Values.get(9)));
        Assert.assertEquals("嘿嘿嘿text", new String((byte[]) beforePresentRows1Values.get(10)));
        Assert.assertEquals("嘿嘿嘿longtext", new String((byte[]) beforePresentRows1Values.get(11)));
        Assert.assertEquals(assertBinary, new String((byte[]) beforePresentRows1Values.get(12)));
    }

    private static final String assertBinary = new String(
            BytesUtil.toBytesFromHexString(
                    "8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547"
            )
    );

    /**
     * insert into `drc4`.`charset_type` values (
     * '嘿嘿嘿varchar4000', '嘿嘿嘿char1000', 'varbinary1800',
     * x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
     * x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
     * x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
     * x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
     * x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
     * '嘿嘿嘿tinytext', '嘿嘿嘿mediumtext', '嘿嘿嘿text', '嘿嘿嘿longtext',
     * x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547'
     * );
     */
    private ByteBuf initCharsetTypeByteBuf() {
        String hexString =
                "c6 b9 a5 5d 1e 01 00 00  00 ab 02 00 00 02 57 00" +
                        "00 00 00 5a 04 00 00 00  00 01 00 02 00 0d ff ff" +
                        "00 e0 14 00 e5 98 bf e5  98 bf e5 98 bf 76 61 72" +
                        "63 68 61 72 34 30 30 30  11 00 e5 98 bf e5 98 bf" +
                        "e5 98 bf 63 68 61 72 31  30 30 30 0d 00 76 61 72" +
                        "62 69 6e 61 72 79 31 38  30 30 53 8b 5f a5 5d 13" +
                        "01 00 00 00 53 00 00 00  46 18 00 00 00 00 3e 04" +
                        "00 00 00 00 01 00 04 64  72 63 34 00 0c 63 68 61" +
                        "72 73 65 74 5f 74 79 70  65 00 0c 0f fe 0f fe fc" +
                        "fc fc fc fc fc fc fc 10  a0 0f ce e8 08 07 fe c8" +
                        "01 03 02 04 01 03 02 04  ff 0f 12 f9 35 47 53 8b" +
                        "5f a5 5d 13 01 00 00 00  53 00 00 00 46 18 00 00" +
                        "00 00 3e 04 00 00 00 00  01 00 04 64 72 63 34 00" +
                        "0c 63 68 61 72 73 65 74  5f 74 79 70 65 00 0c 0f" +
                        "fe 0f fe fc fc fc fc fc  fc fc fc 10 a0 0f ce e8" +
                        "08 07 fe c8 01 03 02 04  01 03 02 04 ff 0f 12 f9" +
                        "35 47 53 00 00 8b 5f a5  5d 13 01 00 00 00 53 00" +
                        "00 00 46 18 00 00 00 00  3e 04 00 00 00 00 01 00" +
                        "04 64 72 63 34 00 0c 63  68 61 72 73 65 74 5f 74" +
                        "79 70 65 00 0c 0f fe 0f  fe fc fc fc fc fc fc fc" +
                        "fc 10 a0 0f ce e8 08 07  fe c8 01 03 02 04 01 03" +
                        "02 04 ff 0f 12 f9 35 47  53 00 8b 5f a5 5d 13 01" +
                        "00 00 00 53 00 00 00 46  18 00 00 00 00 3e 04 00" +
                        "00 00 00 01 00 04 64 72  63 34 00 0c 63 68 61 72" +
                        "73 65 74 5f 74 79 70 65  00 0c 0f fe 0f fe fc fc" +
                        "fc fc fc fc fc fc 10 a0  0f ce e8 08 07 fe c8 01" +
                        "03 02 04 01 03 02 04 ff  0f 12 f9 35 47 53 00 00" +
                        "00 8b 5f a5 5d 13 01 00  00 00 53 00 00 00 46 18" +
                        "00 00 00 00 3e 04 00 00  00 00 01 00 04 64 72 63" +
                        "34 00 0c 63 68 61 72 73  65 74 5f 74 79 70 65 00" +
                        "0c 0f fe 0f fe fc fc fc  fc fc fc fc fc 10 a0 0f" +
                        "ce e8 08 07 fe c8 01 03  02 04 01 03 02 04 ff 0f" +
                        "12 f9 35 47 11 e5 98 bf  e5 98 bf e5 98 bf 74 69" +
                        "6e 79 74 65 78 74 13 00  00 e5 98 bf e5 98 bf e5" +
                        "98 bf 6d 65 64 69 75 6d  74 65 78 74 0d 00 e5 98" +
                        "bf e5 98 bf e5 98 bf 74  65 78 74 11 00 00 00 e5" +
                        "98 bf e5 98 bf e5 98 bf  6c 6f 6e 67 74 65 78 74" +
                        "53 00 00 00 8b 5f a5 5d  13 01 00 00 00 53 00 00" +
                        "00 46 18 00 00 00 00 3e  04 00 00 00 00 01 00 04" +
                        "64 72 63 34 00 0c 63 68  61 72 73 65 74 5f 74 79" +
                        "70 65 00 0c 0f fe 0f fe  fc fc fc fc fc fc fc fc" +
                        "10 a0 0f ce e8 08 07 fe  c8 01 03 02 04 01 03 02" +
                        "04 ff 0f 12 f9 35 47 f8  21 1e 87";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(3000);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }


    private List<TableMapLogEvent.Column> mockCharsetTypeTable() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column varchar4000 = new TableMapLogEvent.Column("varchar4000", true, "varchar", "4000", null, null, null, "utf8mb4", "utf8mb4_general_ci", "varchar(1000)", null, null, null);
        TableMapLogEvent.Column char1000 = new TableMapLogEvent.Column("char1000", true, "char", "1000", null, null, null, "utf8mb4", "utf8mb4_general_ci", "char(250)", null, null, null);
        TableMapLogEvent.Column varbinary1800 = new TableMapLogEvent.Column("varbinary1800", true, "varbinary", "1800", null, null, null, null, null, "varbinary(1800)", null, null, null);
        TableMapLogEvent.Column binary200 = new TableMapLogEvent.Column("binary200", true, "binary", "200", null, null, null, null, null, "binary(200)", null, null, null);
        TableMapLogEvent.Column tinyblob = new TableMapLogEvent.Column("tinyblob", true, "tinyblob", "255", null, null, null, null, null, "tinyblob", null, null, null);
        TableMapLogEvent.Column mediumblob = new TableMapLogEvent.Column("mediumblob", true, "mediumblob", "16777215", null, null, null, null, null, "mediumblob", null, null, null);
        TableMapLogEvent.Column blob = new TableMapLogEvent.Column("blob", true, "blob", "65535", null, null, null, null, null, "blob", null, null, null);
        TableMapLogEvent.Column longblob = new TableMapLogEvent.Column("longblob", true, "longblob", "4294967295", null, null, null, null, null, "longblob", null, null, null);
        TableMapLogEvent.Column tinytext = new TableMapLogEvent.Column("tinytext", true, "tinytext", "255", null, null, null, "utf8mb4", "utf8mb4_general_ci", "tinytext", null, null, null);
        TableMapLogEvent.Column mediumtext = new TableMapLogEvent.Column("mediumtext", true, "mediumtext", "16777215", null, null, null, "utf8mb4", "utf8mb4_general_ci", "mediumtext", null, null, null);
        TableMapLogEvent.Column text = new TableMapLogEvent.Column("text", true, "text", "65535", null, null, null, "utf8mb4", "utf8mb4_general_ci", "text", null, null, null);
        TableMapLogEvent.Column longtext = new TableMapLogEvent.Column("longtext", true, "longtext", "4294967295", null, null, null, "utf8mb4", "utf8mb4_general_ci", "longtext", null, null, null);
        TableMapLogEvent.Column longtextwithoutcharset = new TableMapLogEvent.Column("longtextwithoutcharset", true, "longtext", "4294967295", null, null, null, "latin1", "latin1_swedish_ci", "longtext", null, null, null);
        columns.add(varchar4000);
        columns.add(char1000);
        columns.add(varbinary1800);
        columns.add(binary200);
        columns.add(tinyblob);
        columns.add(mediumblob);
        columns.add(blob);
        columns.add(longblob);

        columns.add(tinytext);
        columns.add(mediumtext);
        columns.add(text);
        columns.add(longtext);
        columns.add(longtextwithoutcharset);
        return columns;
    }
}