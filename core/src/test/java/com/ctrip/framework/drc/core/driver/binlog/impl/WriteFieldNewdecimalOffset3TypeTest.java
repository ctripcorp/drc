package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldNewdecimalOffset3TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.newdecimal3(amount) values(123.45);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "4c f6 89 62   1e   ea 0c 00 00   28 00 00 00   84 01 00 00   00 00" +
                "9d 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 7b" +
                "2d 9a 08 84 e3";
        testWriteValue(rowsHexString, "123.45");
    }

    // insert into drc1.newdecimal3(amount) values(-123.45);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "b9 f6 89 62   1e   ea 0c 00 00   28 00 00 00   8d 02 00 00   00 00" +
                "9d 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff 84" +
                "d2 7a 8e 37 9e";
        testWriteValue(rowsHexString, "-123.45");
    }

    // insert into drc1.newdecimal3(amount) values(99999.99);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "f0 f6 89 62   1e   ea 0c 00 00   28 00 00 00   96 03 00 00   00 00" +
                "9d 00 00 00 00 00 01 00  02 00 01 ff fe 81 86 9f" +
                "63 6f 8c cf 00";
        testWriteValue(rowsHexString, "99999.99");
    }

    // insert into drc1.newdecimal3(amount) values(-99999.99);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "1a f7 89 62   1e   ea 0c 00 00   28 00 00 00   9f 04 00 00   00 00" +
                "9d 00 00 00 00 00 01 00  02 00 01 ff fe 7e 79 60" +
                "9c 5c 7e 12 23";
        testWriteValue(rowsHexString, "-99999.99");
    }

    // insert into drc1.newdecimal3(amount) values(0.1);
    @Test
    public void testScale1() throws IOException {
        String rowsHexString = "42 f7 89 62   1e   ea 0c 00 00   28 00 00 00   a8 05 00 00   00 00" +
                "9d 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "0a fb 31 1a a2";
        testWriteValue(rowsHexString, "0.10");
    }

    // insert into drc1.newdecimal3(amount) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "6d f7 89 62   1e   ea 0c 00 00   28 00 00 00   b1 06 00 00   00 00" +
                "9d 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 a5 0a ec d4";
        testWriteValue(rowsHexString, "0.00");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`newdecimal3` (
     *   `amount` decimal(5,2) NOT NULL COMMENT '金额'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='newdecimal3测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "4c f6 89 62   13   ea 0c 00 00   38 00 00 00   5c 01 00 00   00 00" +
                "9d 00 00 00 00 00 01 00  04 64 72 63 31 00 0b 6e" +
                "65 77 64 65 63 69 6d 61  6c 33 00 01 f6 02 07 02" +
                "00 47 21 a5 e3";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "newdecimal3";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("amount", false, "decimal", null, "5", "2", null, null, null, "decimal(5,2)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
