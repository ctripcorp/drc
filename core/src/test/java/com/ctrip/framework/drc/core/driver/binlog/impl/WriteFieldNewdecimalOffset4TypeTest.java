package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldNewdecimalOffset4TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.newdecimal4(amount) values(123);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "74 f8 89 62   1e   ea 0c 00 00   28 00 00 00   e2 08 00 00   00 00" +
                "9e 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "7b 0d e8 d3 34";
        testWriteValue(rowsHexString, "123");
    }

    // insert into drc1.newdecimal4(amount) values(-123);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "19 f9 89 62   1e   ea 0c 00 00   28 00 00 00   eb 09 00 00   00 00" +
                "9e 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "84 33 68 80 5e";
        testWriteValue(rowsHexString, "-123");
    }

    // insert into drc1.newdecimal4(amount) values(99999999);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "67 f9 89 62   1e   ea 0c 00 00   28 00 00 00   f4 0a 00 00   00 00" +
                "9e 00 00 00 00 00 01 00  02 00 01 ff fe 85 f5 e0" +
                "ff 0d 2e 7e 8c";
        testWriteValue(rowsHexString, "99999999");
    }

    // insert into drc1.newdecimal4(amount) values(-99999999);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "89 f9 89 62   1e   ea 0c 00 00   28 00 00 00   fd 0b 00 00   00 00" +
                "9e 00 00 00 00 00 01 00  02 00 01 ff fe 7a 0a 1f" +
                "00 e1 c9 9c 2a";
        testWriteValue(rowsHexString, "-99999999");
    }

    // insert into drc1.newdecimal4(amount) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "ab f9 89 62   1e   ea 0c 00 00   28 00 00 00   06 0d 00 00   00 00" +
                "9e 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 e7 58 8d e2";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`newdecimal4` (
     *   `amount` decimal(8,0) NOT NULL COMMENT '金额'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='newdecimal4测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "74 f8 89 62   13   ea 0c 00 00   38 00 00 00   ba 08 00 00   00 00" +
                "9e 00 00 00 00 00 01 00  04 64 72 63 31 00 0b 6e" +
                "65 77 64 65 63 69 6d 61  6c 34 00 01 f6 02 08 00" +
                "00 93 34 9d 92";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "newdecimal4";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("amount", false, "decimal", null, "8", "0", null, null, null, "decimal(5,2)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
