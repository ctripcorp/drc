package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Oct 16, 2019
 */
public class DecryptedWriteRowsEvent extends ApplierWriteRowsEvent {

    public DecryptedWriteRowsEvent(Columns columns) {
        this.columns = columns;
    }

    @Override
    public List<Boolean> getBeforeRowsKeysPresent() {
        return null;
    }

    @Override
    public List<List<Object>> getBeforePresentRowsValues() {
        return null;
    }

    public static ApplierWriteRowsEvent mockSimpleEvent() {
        final ByteBuf byteBuf = initMinimalRowsByteBuf();
        ApplierWriteRowsEvent event = new ApplierWriteRowsEvent();
        event.read(byteBuf);
        event.setColumns(Columns.from(mockSimpleColumns()));
        return event;
    }

    private static ByteBuf initMinimalRowsByteBuf() {
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

        return byteBuf;
    }

    public static List<TableMapLogEvent.Column> mockSimpleColumns() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, "default");
        Assert.assertFalse(column1.isOnUpdate());
        Assert.assertFalse(column1.isPk());
        Assert.assertFalse(column1.isUk());
        assertEquals("default", column1.getColumnDefault());
        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("name", true, "char", "30", null, null, null, "UTF8", "utf8_unicode_ci", "char(10)", "PRI", null, null);
        Assert.assertFalse(column2.isOnUpdate());
        Assert.assertTrue(column2.isPk());
        Assert.assertFalse(column2.isUk());
        assertEquals(null, column2.getColumnDefault());
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("age", true, "int", null, "10", null, null, null, null, "int(10)", "UNI", null, "one");
        Assert.assertFalse(column3.isOnUpdate());
        Assert.assertFalse(column3.isPk());
        Assert.assertTrue(column3.isUk());
        assertEquals("one", column3.getColumnDefault());
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("varchar", true, "varchar", "258", null, null, null, "UTF8", "utf8_unicode_ci", "varchar(86)", "MUL", null, "two");
        Assert.assertFalse(column4.isOnUpdate());
        Assert.assertFalse(column4.isPk());
        Assert.assertFalse(column4.isUk());
        assertEquals("two", column4.getColumnDefault());

        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columns.add(column4);

        return columns;
    }
}
