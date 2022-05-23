package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// 3bytes
// [-2^23, 2^23 - 1], [-8388608, 8388607]
// unsigned [0, 2^24 - 1], [0, 16777215]
// mysql literal is int, integer
public class WriteFieldInt24TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.int24(id) values(123);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "df f9 85 62   1e   ea 0c 00 00   27 00 00 00   b8 27 00 00   00 00" +
                "82 00 00 00 00 00 01 00  02 00 01 ff fe 7b 00 00" +
                "0b 26 dc b7";
        testWriteValue(rowsHexString, "123");
    }

    // insert into drc1.int24(id) values(-123);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "7a fa 85 62   1e   ea 0c 00 00   27 00 00 00   bc 28 00 00   00 00" +
                "82 00 00 00 00 00 01 00  02 00 01 ff fe 85 ff ff" +
                "3d 61 23 01";
        testWriteValue(rowsHexString, "-123");
    }

    // insert into drc1.int24(id) values(8388607);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "b7 fa 85 62   1e   ea 0c 00 00   27 00 00 00   c0 29 00 00   00 00" +
                "82 00 00 00 00 00 01 00  02 00 01 ff fe ff ff 7f" +
                "d9 18 7b 5f";
        testWriteValue(rowsHexString, "8388607");
    }

    // insert into drc1.int24(id) values(-8388608);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "de fa 85 62   1e   ea 0c 00 00   27 00 00 00   c4 2a 00 00   00 00" +
                "82 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 80" +
                "1e 06 cd 06";
        testWriteValue(rowsHexString, "-8388608");
    }

    // insert into drc1.int24(id) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "0f fb 85 62   1e   ea 0c 00 00   27 00 00 00   c8 2b 00 00   00 00" +
                "82 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "e3 8e 72 c3";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`int24` (
     *   `id` mediumint NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='int24';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "df f9 85 62   13   ea 0c 00 00   30 00 00 00   91 27 00 00   00 00" +
                "82 00 00 00 00 00 01 00  04 64 72 63 31 00 05 69" +
                "6e 74 32 34 00 01 09 00  00 72 4b c2 0d";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "int24";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "mediumint", null, null, null, null, null, null, "mediumint", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
