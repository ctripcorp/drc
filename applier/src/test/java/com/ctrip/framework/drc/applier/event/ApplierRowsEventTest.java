package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.applier.resource.context.DecryptedLinkContextResource;
import com.ctrip.framework.drc.core.driver.binlog.header.RowsEventPostHeader;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @Author Slight
 * Oct 23, 2019
 */
public class ApplierRowsEventTest implements ApplierColumnsRelatedTest {

    @Test
    public void simpleUse() throws Exception {
        TestEvent testEvent = new TestEvent() {
            @Override
            public RowsEventPostHeader getRowsEventPostHeader() {
                return new RowsEventPostHeader();
            }
        };
        DecryptedLinkContextResource linkContext = mock(DecryptedLinkContextResource.class);
        TableKey tableKey = TableKey.from("prod", "hello");
        tableKey.setColumns(columns1());
        when(linkContext.fetchTableKey()).thenReturn(tableKey);
        when(linkContext.fetchColumns(any())).thenReturn(columns1());
        when(linkContext.fetchColumns()).thenReturn(columns1());
        when(linkContext.fetchTableKeyInMap(0L)).thenReturn(tableKey);
        LogEventHeader logEventHeader = spy(new LogEventHeader());
        testEvent.setLogEventHeader(logEventHeader);
        doReturn(101L).when(logEventHeader).getEventSize();
        testEvent.involve(linkContext);
        verify(linkContext, times(1)).fetchColumns(
                eq(TableKey.from("prod", "hello"))
        );
        assertEquals(columns1(), testEvent.getColumns());
    }

    @Test
    public void testTransformMetaAndType() throws Exception {
        ByteBuf byteBuf = initRowsByteBuf();
        TestEvent writeRowsEvent = (TestEvent) new TestEvent() {
            @Override
            public RowsEventPostHeader getRowsEventPostHeader() {
                return new RowsEventPostHeader();
            }
        }.read(byteBuf);
        LinkContext linkContext = mock(LinkContext.class);
        TableKey tableKey = TableKey.from("prod", "hello");
        tableKey.setColumns(mockOriginColumns());
        when(linkContext.fetchTableKey()).thenReturn(tableKey);
        when(linkContext.fetchTableKeyInMap(0L)).thenReturn(tableKey);
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(initTableMapByteBuf());
        List<TableMapLogEvent.Column> columnList = tableMapLogEvent.getColumns();
        when(linkContext.fetchColumns(tableKey)).thenReturn(Columns.from(columnList));
        writeRowsEvent.involve(linkContext);
        writeRowsEvent.tryLoad();
        Assert.assertTrue(writeRowsEvent.isLoaded());
    }

    @Test
    public void testEnum() throws Exception {
        ByteBuf byteBuf = initEnumRowsByteBuf();
        TestEvent writeRowsEvent = (TestEvent) new TestEvent().read(byteBuf);
        LinkContext linkContext = mock(LinkContext.class);
        TableKey tableKey = TableKey.from("prod", "hello");

        when(linkContext.fetchTableKey()).thenReturn(tableKey);
        when(linkContext.fetchColumns(any())).thenReturn(mockEnumOriginColumns());
        when(linkContext.fetchTableKeyInMap(155L)).thenReturn(tableKey);
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(initEnumTableMapByteBuf());
        List<TableMapLogEvent.Column> columnList = tableMapLogEvent.getColumns();
        tableKey.setColumns(Columns.from(columnList));
        writeRowsEvent.involve(linkContext);
        writeRowsEvent.tryLoad();
        List<List<Object>> values = writeRowsEvent.getBeforePresentRowsValues();
        Assert.assertEquals(values.size(), 2);
        List<Object> firstRow = values.get(0);
        Assert.assertArrayEquals(firstRow.toArray(), Lists.newArrayList(new BigDecimal(2),"nike",new BigDecimal("-11111111112222222222333333333344444444445555555555666666666677777"),(short) 1).toArray());
        List<Object> secondRow = values.get(1);
        Assert.assertArrayEquals(secondRow.toArray(), Lists.newArrayList(new BigDecimal(4),"adi",new BigDecimal("-1111111111222222222233333333334444444444555555555566666666667777"),(short) 3).toArray());
        Assert.assertTrue(writeRowsEvent.isLoaded());
    }

    class TestEvent extends FetcherRowsEvent<TransactionContext> {
        @Override
        protected void doApply(TransactionContext context) {

        }
    }

    public static Columns mockDrcColumns() throws IOException {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("id", true, "int", null, "11", null, null, null, null, "int(11)", null, null, null);
        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("one",true, "varchar", "90", null, null, null, "utf8", "utf8_general_ci", "varchar(30)", null, null, "one");
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("two",true, "varchar", "3000", null, null, null, "utf8", "utf8_general_ci", "varchar(1000)", null, null, "two");
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("three",true, "varchar", "210", null, null, null, "utf8", "utf8_general_ci", "varchar(70)", null, null, null);
        TableMapLogEvent.Column column5 = new TableMapLogEvent.Column("four",true, "varchar", "765", null, null, null, "utf8", "utf8_general_ci", "varchar(255)", null, null, null);
        Assert.assertFalse(column1.isPk());
        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columns.add(column4);
        columns.add(column5);
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(0, 0, 0, "drc4", "utf8mb4", columns, null);
        List<TableMapLogEvent.Column> postColumns = tableMapLogEvent.getColumns();
        return Columns.from(postColumns);
    }

    public static Columns mockOriginColumns() throws IOException {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("id", true, "int", null, "11", null, null, null, null, "int(11)", null, null, null);
        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("one",true, "varchar", "120", null, null, null, "utf8mb4", "utf8mb4_general_ci", "varchar(30)", null, null, "one");
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("two",true, "varchar", "4000", null, null, null, "utf8mb4", "utf8mb4_general_ci", "varchar(1000)", null, null, "two");
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("three",true, "varchar", "280", null, null, null, "utf8mb4", "utf8mb4_general_ci", "varchar(70)", null, null, null);
        TableMapLogEvent.Column column5 = new TableMapLogEvent.Column("four",true, "varchar", "1020", null, null, null, "utf8mb4", "utf8mb4_general_ci", "varchar(255)", null, null, null);
        Assert.assertFalse(column1.isPk());
        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columns.add(column4);
        columns.add(column5);
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(0, 0, 0, "drc4", "utf8mb4", columns, null);
        List<TableMapLogEvent.Column> postColumns = tableMapLogEvent.getColumns();
        return Columns.from(postColumns);
    }

    /**
     * CREATE TABLE `drc4`.`utf8mb4` (
     *                         `id` int(11) NOT NULL AUTO_INCREMENT,
     *                         `one` varchar(30) DEFAULT "one",
     *                         `two` varchar(1000) DEFAULT "two",
     *                         `three` char(70),
     *                         `four` char(255),
     *                         PRIMARY KEY (`id`)
     *                         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
     *
     * insert into `utf8mb4` (`one`, `two`, `three`, `four`) values("abcdefg","hijklmn","opqrst", "uvwxyz"),("abcdefg2","hijklmn2","opqrst2", "uvwxyz2");
     * @return
     */
    private ByteBuf initRowsByteBuf() {
        String hexString = "49 fe 9f 5f 1e 64 00 00 00 73 00 00 00 68 1b 00 00 00 00" +
                "8e 00 00 00 00 00 01 00 02 00 05 ff e0 08 00 00" +
                "00 07 61 62 63 64 65 66 67 07 00 68 69 6a 6b 6c" +
                "6d 6e 06 00 6f 70 71 72 73 74 06 00 75 76 77 78" +
                "79 7a e0 0a 00 00 00 08 61 62 63 64 65 66 67 32" +
                "08 00 68 69 6a 6b 6c 6d 6e 32 07 00 6f 70 71 72" +
                "73 74 32 07 00 75 76 77 78 79 7a 32 27 cb 7d 0c";
        hexString = hexString.replaceAll(" ", "");
        byte[] bytes = HexUtils.fromHexString(hexString);
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    private ByteBuf initTableMapByteBuf() {
        String hexString = "49 fe 9f 5f 13 64 00 00 00 3e 00 00 00 f5 1a 00 00 00 00" +
                "8e 00 00 00 00 00 01 00 04 64 72 63 34 00 07 75" +
                "74 66 38 6d 62 34 00 05 03 0f 0f fe fe 08 78 00" +
                "a0 0f ee 18 ce fc 1e e7 e0 57 55";
        hexString = hexString.replaceAll(" ", "");
        byte[] bytes = HexUtils.fromHexString(hexString);
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    /**
     * CREATE TABLE enum (
     *      id BIGINT UNSIGNED  PRIMARY KEY AUTO_INCREMENT,
     *      brand VARCHAR(255) NOT NULL,
     *      decimal_m_max_d_min_nagetive decimal(65,0) DEFAULT NULL,
     *      color ENUM('RED','GREEN','BLUE')
     *   ) ENGINE = InnoDB;
     *
     * insert into enum (`brand`,`decimal_m_max_d_min_nagetive`,`color`) values("nike",-11111111112222222222333333333344444444445555555555666666666677777,"RED"),("adi",-1111111111222222222233333333334444444444555555555566666666667777,"BLUE");
     * @return
     */
    private ByteBuf initEnumRowsByteBuf() {
        String hexString = "e8 d3 a0 5f 1e 64 00 00 00 7a 00 00 00 3f 1e 00 00 00 00" +
                "9b 00 00 00 00 00 01 00 02 00 04 ff f0 02 00 00" +
                "00 00 00 00 00 04 6e 69 6b 65 74 f9 60 94 37 f2" +
                "c1 28 71 ec 21 bc aa eb 78 31 e3 e5 71 5c 9c de" +
                "e1 33 15 d8 43 4d ee 01 f0 04 00 00 00 00 00 00" +
                "00 03 61 64 69 7e f9 60 94 38 f2 c1 28 71 f2 17" +
                "9d aa ec 10 c8 63 e5 80 9e dc de e2 b9 b5 d8 43" +
                "74 fe 03 2c 2f 1a 4c";
        hexString = hexString.replaceAll(" ", "");
        byte[] bytes = HexUtils.fromHexString(hexString);
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    private ByteBuf initEnumTableMapByteBuf() {
        String hexString = "e8 d3 a0 5f 13 64 00 00 00 38 00 00 00 c5 1d 00 00 00 00" +
                "9b 00 00 00 00 00 01 00 04 64 72 63 34 00 04 65" +
                "6e 75 6d 00 04 08 0f f6 fe 06 ff 00 41 00 f7 01" +
                "0c 13 2d a5 4b";
        hexString = hexString.replaceAll(" ", "");
        byte[] bytes = HexUtils.fromHexString(hexString);
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    public static Columns mockEnumOriginColumns() throws IOException {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("id", false, "bigint", null, "20", "0", null, null, null, "bigint(20) unsigned", null, null, null);
        TableMapLogEvent.Column column2 = new TableMapLogEvent.Column("brand",false, "varchar", "255", null, null, null, "latin1", "latin1_swedish_ci", "varchar(255)", null, null, null);
        TableMapLogEvent.Column column3 = new TableMapLogEvent.Column("decimal_m_max_d_min_nagetive",true, "decimal", null, "65", "0", null, null, null, "decimal(65,0)", null, null, null);
        TableMapLogEvent.Column column4 = new TableMapLogEvent.Column("color",true, "enum", "5", null, null, null, "latin1", "latin1_swedish_ci", "enum('RED','GREEN','BLUE')", null, null, null);
        Assert.assertFalse(column1.isPk());
        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columns.add(column4);
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(0, 0, 0, "drc4", "utf8mb4", columns, null);
        List<TableMapLogEvent.Column> postColumns = tableMapLogEvent.getColumns();
        return Columns.from(postColumns);
    }

}
