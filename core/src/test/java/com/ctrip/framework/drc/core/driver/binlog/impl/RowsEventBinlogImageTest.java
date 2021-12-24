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
public class RowsEventBinlogImageTest {

    @Test
    public void readFullRowsEventTest() {
        final ByteBuf byteBuf = initFullByteBufBug();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);

        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, null);
        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("user", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, null);
        columns.add(column1);
        columns.add(column2);
        writeRowsEvent.load(columns);

        final List<Boolean> beforeRowsKeysPresent = writeRowsEvent.getBeforeRowsKeysPresent();
        Assert.assertEquals(2, beforeRowsKeysPresent.size());
        Assert.assertTrue(beforeRowsKeysPresent.get(0));
        Assert.assertTrue(beforeRowsKeysPresent.get(1));

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforePresentRowsValues.size());

        final List<Object> beforePresentRows1Values = beforePresentRowsValues.get(0);
        Assert.assertEquals(2, beforePresentRows1Values.size());
        Assert.assertNull(beforePresentRows1Values.get(0));
        Assert.assertEquals("a", beforePresentRows1Values.get(1));

    }

    private ByteBuf initFullByteBufBug() {
        String hexString =
                "89 ed 9e 5d 1e 64 00 00  00 26 00 00 00 dd 11 00" +
                        "00 00 00 " +
                        "6c 00 00 00 00  00 01 00 02 00 " +
                        "02 ff fd" +
                        "01 61 97 42 2c 22";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
