package com.ctrip.framework.drc.core.driver.binlog;

import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.ctrip.framework.drc.core.driver.binlog.impl.BytesUtil;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.WriteRowsEvent;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType.mysql_type_set;

/**
 * https://dev.mysql.com/doc/refman/8.0/en/set.html
 *
 * Created by dengquanliang
 * 2024/12/31 15:08
 */
public class SetTypeTest {

    private static List<Integer> SET_NUMS = Lists.newArrayList(8, 16, 24, 32, 40, 48, 56, 64);

    @Test
    public void test() {
        byte[] bits = new byte[]{-77, 83, 119, 103, 30, 0, 13, 0, 0, 94, 0, 0, 0, 49, -113, -29, 3, 0, 0,-60, -54, 4, 0, 0, 0, 1, 0, 2, 0, 10, -1, -1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, -127, 2, -128, 4, 0, -64, 0, -128, -126, -128, 64, 0, 0, 0, -128, 0, 0, 0, 0, 0, -128, 40, 0, -128, 0, 0, 0, 0, 0, 0, 0, -128, -120, 0, -120, 0, 0, 0, 0, 0, 0, -32, 103, 119, 83, 51, 38, -14, -74, 12, -96, 118};
        String str = HexUtils.toHexString(bits);
        System.out.println(str);
    }

    @Test
    public void testSetColumn() {
        TableMapLogEvent.Column setColumn = new TableMapLogEvent.Column("set1", true, "set", null, null, null, null, null, null, "set('(a1)','a2)','(a3')", null, null, null);
        Assert.assertEquals(1, setColumn.getMeta());
    }

    @Test
    public void testRead() {
        final ByteBuf byteBuf = initBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        writeRowsEvent.load(mockTableInfo());

        final List<List<Object>> beforePresentRowsValues = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(1,beforePresentRowsValues.size());

        final List<Object> values = beforePresentRowsValues.get(0);
        Assert.assertEquals(values.size(), 10);
        Assert.assertEquals((short) 129, values.get(1));
        Assert.assertEquals(32770, values.get(2));
        Assert.assertEquals(new BigDecimal("16140901064495857800"), values.get(8));
        Assert.assertEquals("2025-01-03 11:02:11.997", values.get(9));
    }



    private List<TableMapLogEvent.Column> mockTableInfo() {

        TableMapLogEvent.Column id = new TableMapLogEvent.Column("id", false, "bigint", null, null, null, null, null, null, "int", "PRI", "auto_increment", "NULL");

        TableMapLogEvent.Column set8 = new TableMapLogEvent.Column("set8", true, "set", null, null, null, null, null, null, getColumnType(8), null, null, "a8");
        TableMapLogEvent.Column set16 = new TableMapLogEvent.Column("set16", true, "set", null, null, null, null, null, null, getColumnType(16), null, null, "a16");
        TableMapLogEvent.Column set24 = new TableMapLogEvent.Column("set24", true, "set", null, null, null, null, null, null, getColumnType(24), null, null, "a24");
        TableMapLogEvent.Column set32 = new TableMapLogEvent.Column("set32", true, "set", null, null, null, null, null, null, getColumnType(32), null, null, "a32");
        TableMapLogEvent.Column set40 = new TableMapLogEvent.Column("set40", true, "set", null, null, null, null, null, null, getColumnType(40), null, null, "a40");
        TableMapLogEvent.Column set48 = new TableMapLogEvent.Column("set48", true, "set", null, null, null, null, null, null, getColumnType(48), null, null, "a48");
        TableMapLogEvent.Column set56 = new TableMapLogEvent.Column("set56", true, "set", null, null, null, null, null, null, getColumnType(56), null, null, "a56");
        TableMapLogEvent.Column set64 = new TableMapLogEvent.Column("set64", true, "set", null, null, null, null, null, null, getColumnType(64), null, null, "a64");
        TableMapLogEvent.Column time = new TableMapLogEvent.Column("datachange_lasttime", false, "timestamp", null, null, null, "3", null, null, "timestamp(3)", null, "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3) ");
        List<TableMapLogEvent.Column> columns = Lists.newArrayList(id, set8, set16, set24, set32, set40, set48, set56, set64, time);
        return columns;
    }

    @Test
    public void testColumn() {
        for (int num : SET_NUMS) {
            TableMapLogEvent.Column column = new TableMapLogEvent.Column("column_set", true, "set", null, null, null, null, null, null, getColumnType(num), null, null, "a1");
            if (num <= 32) {
                Assert.assertEquals(num / 8, column.getMeta());
            } else {
                Assert.assertEquals(8, column.getMeta());
            }

        }
    }

    private String getColumnType(int num) {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= num; i++) {
            builder.append("'a" + i + "'").append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        return String.format("set(%s)", builder.toString());
    }

    /**
     * CREATE TABLE `table_set` (
     *   `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
     *   `set8` set('a1','a2','a3','a4','a5','a6','a7','a8') NOT NULL DEFAULT 'a8' COMMENT '测试set',
     *   `set16` set('a1','a2','a3','a4','a5','a6','a7','a8', ..., 'a15','a16') NOT NULL DEFAULT 'a8' COMMENT '测试set',
     *   `set24` set('a1','a2','a3','a4','a5','a6','a7','a8', ..., 'a23','a24') NOT NULL DEFAULT 'a8' COMMENT '测试set',
     *   ...
     *   `set64` set('a1','a2','a3','a4','a5','a6','a7','a8', ..., 'a63', 'a64') NOT NULL DEFAULT 'a8' COMMENT '测试set',
     *   PRIMARY KEY (`id`),
     * ) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb3 COMMENT='test_set';
     */
    @Test
    public void testSetType() {
        final ByteBuf byteBuf = iniSetTypeBuf();
        final TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        final List<TableMapLogEvent.Column> columns = tableMapLogEvent.getColumns();

        Assert.assertEquals("table_set", tableMapLogEvent.getTableName());
        Assert.assertEquals(9, columns.size());
        final TableMapLogEvent.Column set8 = columns.get(1);
        final TableMapLogEvent.Column set16 = columns.get(2);
        final TableMapLogEvent.Column set24 = columns.get(3);
        final TableMapLogEvent.Column set32 = columns.get(4);
        final TableMapLogEvent.Column set40 = columns.get(5);
        final TableMapLogEvent.Column set48 = columns.get(6);
        final TableMapLogEvent.Column set56 = columns.get(7);
        final TableMapLogEvent.Column set64 = columns.get(8);

        Assert.assertEquals(mysql_type_set, MysqlFieldType.getMysqlFieldType(set8.getType()));
        Assert.assertEquals(mysql_type_set, MysqlFieldType.getMysqlFieldType(set16.getType()));
        Assert.assertEquals(mysql_type_set, MysqlFieldType.getMysqlFieldType(set24.getType()));
        Assert.assertEquals(mysql_type_set, MysqlFieldType.getMysqlFieldType(set32.getType()));
        Assert.assertEquals(mysql_type_set, MysqlFieldType.getMysqlFieldType(set40.getType()));
        Assert.assertEquals(mysql_type_set, MysqlFieldType.getMysqlFieldType(set48.getType()));
        Assert.assertEquals(mysql_type_set, MysqlFieldType.getMysqlFieldType(set56.getType()));
        Assert.assertEquals(mysql_type_set, MysqlFieldType.getMysqlFieldType(set64.getType()));

        Assert.assertEquals(1, set8.getMeta());
        Assert.assertEquals(2, set16.getMeta());
        Assert.assertEquals(3, set24.getMeta());
        Assert.assertEquals(4, set32.getMeta());
        Assert.assertEquals(8, set40.getMeta());
        Assert.assertEquals(8, set48.getMeta());
        Assert.assertEquals(8, set56.getMeta());
        Assert.assertEquals(8, set64.getMeta());
    }

    private ByteBuf initBuf() {
        return iniSetTypeBuf("b35377671e000d00005e000000318fe3030000c4ca04000000010002000affff000001000000000000008102800400c00080828040000000800000000000802800800000000000000080880088000000000000e06777533326f2b60ca076");
    }

    private ByteBuf iniSetTypeBuf() {
        return iniSetTypeBuf("5e7176676400000000b1000000b100000080000000000000008000097465737473657464620" +
                "0097461626c655f736574000908f8f8f8f8f8f8f8f8100100020003000400080008000800080000000269640473657438" +
                "057365743136057365743234057365743332057365743430057365743438057365743536057365743634ff01ff01000000" +
                "000100000000000100026138026138026138026138026138026138026138026138010102696400000000");
    }


    private ByteBuf iniSetTypeBuf(String hexString) {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(400);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
//        final byte[] bytes = new byte[]{-76, -72, 115, 103, 100, 0, 0, 0, 0, 118, 0, 0, 0, 118, 0, 0, 0, -128, 0,0, 0, 0, 0, 0, 0, -128, 0, 10, 100, 114, 99, 116, 101, 115, 116, 115, 101, 116, 0, 10, 116, 101, 115, 116, 95, 101, 110, 117, 109, 52, 0, 3, 8, 15, -9, 4, 120, 0, 1, 0, 6, 2, 105, 100, 4, 110, 97, 109, 101, 4, 100, 97, 116, 97, 5, 7, 117, 116, 102, 56, 109, 98, 51, 5, 18, 117, 116, 102, 56, 109, 98, 51, 95, 103, 101, 110, 101, 114, 97, 108, 95, 99, 105, 0, 0, 1, 0, 0, 7, 1, 1, 2, 105, 100, 0, 0, 0, 0};
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
