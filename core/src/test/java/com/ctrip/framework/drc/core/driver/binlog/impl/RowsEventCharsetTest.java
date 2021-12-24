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
public class RowsEventCharsetTest {

    @Test
    public void readJapanCharsetMinimalTest() {
        final ByteBuf byteBuf = initJapanCharsetMinimalByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);

        List<TableMapLogEvent.Column> columns = Lists.newArrayList();

        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("id", false, "int", null, "10", null, null, "cp932", "cp932_japanese_ci", "int(10) unsigned", null, null, "NULL");
        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("string", true, "char", "20", null, null, null, "cp932", "cp932_japanese_ci", "char(10)", null, null, "NULL");
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("varchar", true, "varchar", "20", null, null, null, "cp932", "cp932_japanese_ci", "varchar(10)", null, null, "NULL");
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("string1", true, "char", "20", null, null, null, "cp932", "cp932_japanese_ci", "char(10)", null, null, "NULL");
        TableMapLogEvent.Column column5 = new TableMapLogEvent.Column("varchar1", true, "varchar", "20", null, null, null, "cp932", "cp932_japanese_ci", "varchar(10)", null, null, "NULL");
        TableMapLogEvent.Column column6 = new TableMapLogEvent.Column("string2", true, "char", "20", null, null, null, "cp932", "cp932_japanese_ci", "char(10)", null, null, "NULL");
        TableMapLogEvent.Column column7 = new TableMapLogEvent.Column("varchar2", true, "varchar", "20", null, null, null, "cp932", "cp932_japanese_ci", "varchar(10)", null, null, "NULL");

        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columns.add(column4);
        columns.add(column5);
        columns.add(column6);
        columns.add(column7);
        writeRowsEvent.load(columns);

        final List<Boolean> beforeRowsKeysPresent = writeRowsEvent.getBeforeRowsKeysPresent();
        Assert.assertEquals(7, beforeRowsKeysPresent.size());
        Assert.assertTrue(beforeRowsKeysPresent.get(0));
        Assert.assertTrue(beforeRowsKeysPresent.get(1));
        Assert.assertTrue(beforeRowsKeysPresent.get(2));
        Assert.assertFalse(beforeRowsKeysPresent.get(3));
        Assert.assertTrue(beforeRowsKeysPresent.get(4));
        Assert.assertFalse(beforeRowsKeysPresent.get(5));
        Assert.assertTrue(beforeRowsKeysPresent.get(6));

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforePresentRowsValues.size());

        final List<Object> beforePresentRows1Values = beforePresentRowsValues.get(0);
        Assert.assertEquals(5, beforePresentRows1Values.size());
        Assert.assertEquals(32L, beforePresentRows1Values.get(0));
        Assert.assertEquals("今はヌ", beforePresentRows1Values.get(1));
        Assert.assertEquals("今はヌ", beforePresentRows1Values.get(2));
        Assert.assertEquals("今はヌ", beforePresentRows1Values.get(3));
        Assert.assertEquals("今はヌ", beforePresentRows1Values.get(4));
    }

    /**
     * insert into charset_test2.charset_japan2 (`string`, `varchar`, `varchar1`, `varchar2`) values ('今はヌ', '今はヌ', '今はヌ', '今はヌ');
     */
    private ByteBuf initJapanCharsetMinimalByteBuf() {
        String hexString =
                "62 33 9f 5d 1e 01 00 00  00 44 00 00 00 19 2b 00" +
                        "00 00 00 7c 00 00 00 00  00 01 00 02 00 07 57 e0" +
                        "20 00 00 00 06 8d a1 82  cd 83 6b 06 8d a1 82 cd" +
                        "83 6b 06 8d a1 82 cd 83  6b 06 8d a1 82 cd 83 6b" +
                        "11 c9 1e 10";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}