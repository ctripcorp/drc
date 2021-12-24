package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.write_rows_event_v2;

/**
 * Created by @author zhuYongMing on 2019/9/15.
 */
public class WriteRowsEventTest {

    @Test
    public void readMinimalRowsEventTest() {
        final ByteBuf byteBuf = initMinimalRowsByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        if (null == writeRowsEvent) {
            Assert.fail();
        }

        if (null == writeRowsEvent.getLogEventHeader()) {
            Assert.fail();
        }

        writeRowsEvent.load(TableMapLogEventTest.mockColumns());
        if (null == writeRowsEvent.getRowsEventPostHeader()) {
            Assert.fail();
        }

        // valid decode
        Assert.assertEquals(write_rows_event_v2, LogEventType.getLogEventType(writeRowsEvent.getLogEventHeader().getEventType()));
        Assert.assertEquals(4, writeRowsEvent.getNumberOfColumns());

        // beforePresentBitMap
        final BitSet beforePresentBitMap = writeRowsEvent.getBeforePresentBitMap();
        Assert.assertTrue(beforePresentBitMap.get(0));
        Assert.assertTrue(!beforePresentBitMap.get(1));
        Assert.assertTrue(!beforePresentBitMap.get(2));
        Assert.assertTrue(beforePresentBitMap.get(3));
        Assert.assertTrue(!beforePresentBitMap.get(4));
        Assert.assertTrue(!beforePresentBitMap.get(5));
        Assert.assertTrue(!beforePresentBitMap.get(6));
        Assert.assertTrue(!beforePresentBitMap.get(7));
        Assert.assertEquals(4, beforePresentBitMap.length());

        final List<Boolean> beforeRowsKeysPresent = writeRowsEvent.getBeforeRowsKeysPresent();
        Assert.assertEquals(4, beforeRowsKeysPresent.size());
        Assert.assertTrue(beforeRowsKeysPresent.get(0));
        Assert.assertFalse(beforeRowsKeysPresent.get(1));
        Assert.assertFalse(beforeRowsKeysPresent.get(2));
        Assert.assertTrue(beforeRowsKeysPresent.get(3));

        // afterPresentBitMap
        Assert.assertNull(writeRowsEvent.getAfterPresentBitMap());

        // beforeNullBitMap
        final AbstractRowsEvent.Row row = writeRowsEvent.getRows().get(0);
        final BitSet beforeNullBitMap = row.getBeforeNullBitMap();
        Assert.assertTrue(!beforeNullBitMap.get(0));
        Assert.assertTrue(!beforeNullBitMap.get(1));
        Assert.assertTrue(beforeNullBitMap.get(2));
        Assert.assertTrue(beforeNullBitMap.get(3));
        Assert.assertTrue(beforeNullBitMap.get(4));
        Assert.assertTrue(beforeNullBitMap.get(5));
        Assert.assertTrue(beforeNullBitMap.get(6));
        Assert.assertTrue(beforeNullBitMap.get(7));
        // beforeValues
        final List<Object> beforeValues = row.getBeforeValues();
        Assert.assertEquals(9, beforeValues.get(0));
        Assert.assertEquals("varchar", beforeValues.get(1));

        final List<List<Object>> beforeRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforeRowsValues.size());

        final List<Object> beforeRow1Values = beforeRowsValues.get(0);
        Assert.assertEquals(2, beforeRow1Values.size());
        Assert.assertEquals(9, beforeRow1Values.get(0));
        Assert.assertEquals("varchar", beforeRow1Values.get(1));

        // afterNullBitMap
        final BitSet afterNullBitMap = row.getAfterNullBitMap();
        Assert.assertNull(afterNullBitMap);
        // afterValues
        Assert.assertNull(row.getAfterValues());

        Assert.assertEquals(49, byteBuf.readerIndex());
    }

    /**
     * binlog_row_image : minimal
     * insert into gtid_test.row_image3(`id`, `varchar`) values (9, 'varchar')
     * <p>
     * # at 1676
     * #190915 23:49:37 server id 1  end_log_pos 1725 CRC32 0x80e874b8         Write_rows: table id 123 flags: STMT_END_F
     * ### INSERT INTO `gtid_test`.`row_image3`
     * ### SET
     * ###   @1=9 INT schema=0 nullable=1 is_null=0
     * ###   @4='varchar' VARSTRING(258) schema=258 nullable=1 is_null=0
     * <p>
     * 0000068c  91 5d 7e 5d 1e 01 00 00  00 31 00 00 00 bd 06 00  |.]~].....1......|
     * 0000069c  00 00 00 7b 00 00 00 00  00 01 00 02 00 04 09 fc  |...{............|
     * 000006ac  09 00 00 00 07 00 76 61  72 63 68 61 72 b8 74 e8  |......varchar.t.|
     * 000006bc  80
     */
    private ByteBuf initMinimalRowsByteBuf() {
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

    @Test
    public void getBeforeDataTest() {
        final ByteBuf byteBuf = initMinimalByteBufTableHasDefaultValue();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        if (null == writeRowsEvent) {
            Assert.fail();
        }

        if (null == writeRowsEvent.getLogEventHeader()) {
            Assert.fail();
        }

        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("id",false, "int", null, "10",  null, null, null, null, "int(11)", null, null, null);
        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("first",true, "varchar", "30", null, null,  null, "utf8", "utf8_general_ci", "varchar(30)", null, null, null);
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("second",true, "int", null, "10", null, null, null, null, "int(20)", null, null, null);
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("three",true, "varchar", "30", null, null, null, "utf8", "utf8_general_ci", "varchar(30)", null, null, null);
        TableMapLogEvent.Column column5 = new TableMapLogEvent.Column("fou",true, "int", null, "10",  null, null, null, null, "int(20)", null, null, null);
        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columns.add(column4);
        columns.add(column5);

        writeRowsEvent.load(columns);
        if (null == writeRowsEvent.getRowsEventPostHeader()) {
            Assert.fail();
        }

        final List<Boolean> beforeRowsKeysPresent = writeRowsEvent.getBeforeRowsKeysPresent();
        Assert.assertEquals(5, beforeRowsKeysPresent.size());
        Assert.assertTrue(beforeRowsKeysPresent.get(0));
        Assert.assertTrue(beforeRowsKeysPresent.get(1));
        Assert.assertFalse(beforeRowsKeysPresent.get(2));
        Assert.assertFalse(beforeRowsKeysPresent.get(3));
        Assert.assertFalse(beforeRowsKeysPresent.get(4));

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1, beforePresentRowsValues.size());

        final List<Object> beforePresentRows1Values = beforePresentRowsValues.get(0);
        Assert.assertEquals(2, beforePresentRows1Values.size());
        Assert.assertEquals(6, beforePresentRows1Values.get(0));
        Assert.assertNull(beforePresentRows1Values.get(1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getAfterRowsKeysPresentTest() {
        final ByteBuf byteBuf = initMinimalByteBufTableHasDefaultValue();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        final List<Boolean> afterRowsKeysPresent = writeRowsEvent.getAfterRowsKeysPresent();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getAfterPresentRowsValuesTest() {
        final ByteBuf byteBuf = initMinimalByteBufTableHasDefaultValue();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        final List<List<Object>> afterPresentRowsValues = writeRowsEvent.getAfterPresentRowsValues();
    }

    /**
     * table:
     * CREATE TABLE `percona`.`default_value2` (
     * `id` int(11) NOT NULL AUTO_INCREMENT,
     * `first` varchar(30) DEFAULT "",
     * `second` int(20) DEFAULT 0,
     * `three` varchar(30) DEFAULT "three",
     * `four` int(20),
     * PRIMARY KEY (`id`)
     * ) COMMENT='';
     * <p>
     * sql:
     * insert into percona.default_value2 (`first`) values (null)
     * <p>
     * binlog:
     * # at 842
     * #190926 19:36:15 server id 1  end_log_pos 882 CRC32 0xf96adff3  Write_rows: table id 108 flags: STMT_END_F
     * ### INSERT INTO `percona`.`default_value2`
     * ### SET
     * ###   @1=6  INT schema=0 nullable=0 is_null=0
     * ###   @2=NULL  VARSTRING(30) schema=30 nullable=1 is_null=1
     * <p>
     * binary:
     * 0000034a  af a2 8c 5d 1e 01 00 00  00 28 00 00 00 72 03 00  |...].....(...r..|
     * 0000035a  00 00 00 6c 00 00 00 00  00 01 00 02 00 05 03 fe  |...l............|
     * 0000036a  06 00 00 00 f3 df 6a f9
     */
    private ByteBuf initMinimalByteBufTableHasDefaultValue() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(40);
        byte[] bytes = new byte[]{
                (byte) 0xaf, (byte) 0xa2, (byte) 0x8c, (byte) 0x5d, (byte) 0x1e, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x28, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x72, (byte) 0x03, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x6c, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x05, (byte) 0x03, (byte) 0xfe,

                (byte) 0x06, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xf3, (byte) 0xdf, (byte) 0x6a, (byte) 0xf9
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
