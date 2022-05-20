package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/18
 */
// 4bytes
// [-2^31, 2^31 - 1], [-2147483648, 2147483647]
// unsigned [0, 2^32 - 1], [0, 4294967295]
// mysql literal is int
public class WriteFieldLongTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.long(id) values(12);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "25 99 84 62   1e   ea 0c 00 00   28 00 00 00   0a 0c 00 00   00 00" +
                "76 00 00 00 00 00 01 00  02 00 01 ff fe 0c 00 00" +
                "00 bb 4c 9d 91";
        testWriteValue(rowsHexString, "12");
    }

    // insert into drc1.long(id) values(-12);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "bc 9a 84 62   1e   ea 0c 00 00   28 00 00 00   0a 0d 00 00   00 00" +
                "76 00 00 00 00 00 01 00  02 00 01 ff fe f4 ff ff" +
                "ff 3b 02 54 2d";
        testWriteValue(rowsHexString, "-12");
    }

    // insert into drc1.long(id) values(2147483647);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "19 9c 84 62   1e   ea 0c 00 00   28 00 00 00   0a 10 00 00   00 00" +
                "76 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "7f f7 a2 1b 53";
        testWriteValue(rowsHexString, "2147483647");
    }

    // insert into drc1.long(id) values(-2147483648);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "93 9b 84 62   1e   ea 0c 00 00   28 00 00 00   0a 0f 00 00   00 00" +
                "76 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "80 0f a8 7b 1d";
        testWriteValue(rowsHexString, "-2147483648");
    }

    // insert into drc1.long(id) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "35 9b 84 62   1e   ea 0c 00 00   28 00 00 00   0a 0e 00 00   00 00" +
                "76 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 0e 71 17 50";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`long` (
     *   `id` int NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='long测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "25 99 84 62   13   ea 0c 00 00   2f 00 00 00   e2 0b 00 00   00 00" +
                "76 00 00 00 00 00 01 00  04 64 72 63 31 00 04 6c" +
                "6f 6e 67 00 01 03 00 00  a4 5c 39 c5";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "long";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "int", null, null, null, null, null, null, "int", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
