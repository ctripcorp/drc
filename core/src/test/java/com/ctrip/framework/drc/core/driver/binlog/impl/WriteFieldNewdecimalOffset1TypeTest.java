package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldNewdecimalOffset1TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.newdecimal1(amount) values(123.456);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "84 ec 89 62   1e   ea 0c 00 00   2d 00 00 00   3d 0b 00 00   00 00" +
                "9c 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 7b 1b 2e 02 00 8c 0b  b0 08";
        testWriteValue(rowsHexString, "123.456000000");
    }

    // insert into drc1.newdecimal1(amount) values(-123.456);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "d7 ed 89 62   1e   ea 0c 00 00   2d 00 00 00   4b 0c 00 00   00 00" +
                "9c 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff 84 e4 d1 fd ff 96 81  26 34";
        testWriteValue(rowsHexString, "-123.456000000");
    }

    // insert into drc1.newdecimal1(amount) values(9999999999.999999999);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "e8 ee 89 62   1e   ea 0c 00 00   2d 00 00 00   59 0d 00 00   00 00" +
                "9c 00 00 00 00 00 01 00  02 00 01 ff fe 89 3b 9a" +
                "c9 ff 3b 9a c9 ff dd b1  c1 78";
        testWriteValue(rowsHexString, "9999999999.999999999");
    }

    // insert into drc1.newdecimal1(amount) values(-9999999999.999999999);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "24 ef 89 62   1e   ea 0c 00 00   2d 00 00 00   67 0e 00 00   00 00" +
                "9c 00 00 00 00 00 01 00  02 00 01 ff fe 76 c4 65" +
                "36 00 c4 65 36 00 99 68  a0 7d";
        testWriteValue(rowsHexString, "-9999999999.999999999");
    }

    // insert into drc1.newdecimal1(amount) values(999999999.999);
    @Test
    public void testInt9() throws IOException {
        String rowsHexString = "3a f0 89 62   1e   ea 0c 00 00   2d 00 00 00   75 0f 00 00   00 00" +
                "9c 00 00 00 00 00 01 00  02 00 01 ff fe 80 3b 9a" +
                "c9 ff 3b 8b 87 c0 e1 d8  28 71";
        testWriteValue(rowsHexString, "999999999.999000000");
    }

    // insert into drc1.newdecimal1(amount) values(0.1);
    @Test
    public void testScale1() throws IOException {
        String rowsHexString = "cd f0 89 62   1e   ea 0c 00 00   2d 00 00 00   83 10 00 00   00 00" +
                "9c 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 05 f5 e1 00 18 e1  5b 39";
        testWriteValue(rowsHexString, "0.100000000");
    }

    // insert into drc1.newdecimal1(amount) values(-0.1);
    @Test
    public void testNegativeScale1() throws IOException {
        String rowsHexString = "ff f0 89 62   1e   ea 0c 00 00   2d 00 00 00   91 11 00 00   00 00" +
                "9c 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff ff fa 0a 1e ff a6 b2  87 a6";
        testWriteValue(rowsHexString, "-0.100000000");
    }

    // insert into drc1.newdecimal1(amount) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "e4 f4 89 62   1e   ea 0c 00 00   2d 00 00 00   e5 15 00 00   00 00" +
                "9c 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 00 00 00 00 54 c7  52 0b";
        testWriteValue(rowsHexString, "0E-9");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`newdecimal1` (
     *   `amount` decimal(19,9) NOT NULL COMMENT '金额'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='newdecimal1测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "84 ec 89 62   13   ea 0c 00 00   38 00 00 00   10 0b 00 00   00 00" +
                "9c 00 00 00 00 00 01 00  04 64 72 63 31 00 0b 6e" +
                "65 77 64 65 63 69 6d 61  6c 31 00 01 f6 02 13 09" +
                "00 9d ab 8f 4a";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "newdecimal1";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("amount", false, "decimal", null, "19", "9", null, null, null, "decimal(19,9)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
