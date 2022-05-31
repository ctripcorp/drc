package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// double (-1.7976931348623157E+308，-2.2250738585072014E-308)，0，(2.2250738585072014E-308，1.7976931348623157E+308)
// see https://bbs.huaweicloud.com/forum/thread-90199-1-1.html
public class WriteFieldDoubleTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.double(amount) values(12.25);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "63 40 86 62   1e   ea 0c 00 00   2c 00 00 00   82 01 00 00   00 00" +
                "8c 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 80 28 40 1c ad 15  74";
        testWriteValue(rowsHexString, "12.25");
    }

    // insert into drc1.double(amount) values(-12.25);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "31 41 86 62   1e   ea 0c 00 00   2c 00 00 00   89 02 00 00   00 00" +
                "8c 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 80 28 c0 6f 11 04  40";
        testWriteValue(rowsHexString, "-12.25");
    }

    // insert into drc1.double(amount) values(-1.7976931348623157E308);
    @Test
    public void testNegativeMin() throws IOException {
        String rowsHexString = "5f 41 86 62   1e   ea 0c 00 00   2c 00 00 00   90 03 00 00   00 00" +
                "8c 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff ff ff ef ff f0 56 00  04";
        testWriteValue(rowsHexString, "-1.7976931348623157E308");
    }

    // insert into drc1.double(amount) values(-2.2250738585072014E-308);
    @Test
    public void testNegativeMax() throws IOException {
        String rowsHexString = "c6 41 86 62   1e   ea 0c 00 00   2c 00 00 00   97 04 00 00   00 00" +
                "8c 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 10 80 b7 fe e4  4c";
        testWriteValue(rowsHexString, "-2.2250738585072014E-308");
    }

    // insert into drc1.double(amount) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "08 42 86 62   1e   ea 0c 00 00   2c 00 00 00   9e 05 00 00   00 00" +
                "8c 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 00 00 00 fd 9e  f3";
        testWriteValue(rowsHexString, "0.0");
    }

    // insert into drc1.double(amount) values(2.2250738585072014E-308);
    @Test
    public void testPositiveMin() throws IOException {
        String rowsHexString = "46 42 86 62   1e   ea 0c 00 00   2c 00 00 00   a5 06 00 00   00 00" +
                "8c 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 10 00 8a e7 f4  92";
        testWriteValue(rowsHexString, "2.2250738585072014E-308");
    }

    // insert into drc1.double(amount) values(1.7976931348623157E308);
    @Test
    public void testPositiveMax() throws IOException {
        String rowsHexString = "8a 42 86 62   1e   ea 0c 00 00   2c 00 00 00   ac 07 00 00   00 00" +
                "8c 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff ff ff ef 7f 2d c8 49  75";
        testWriteValue(rowsHexString, "1.7976931348623157E308");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`double` (
     *  `amount` double NOT NULL COMMENT '价格'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='double测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "63 40 86 62   13   ea 0c 00 00   32 00 00 00   56 01 00 00   00 00" +
                "8c 00 00 00 00 00 01 00  04 64 72 63 31 00 06 64" +
                "6f 75 62 6c 65 00 01 05  01 08 00 ba 1e a4 ad";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "double";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("amount", false, "double", null, null, null, null, null, null, "double", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
