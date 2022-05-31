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
public class WriteFieldInt24UnsignedTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.int24(id) values(123);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "bc fb 85 62   1e   ea 0c 00 00   27 00 00 00   50 03 00 00   00 00" +
                "83 00 00 00 00 00 01 00  02 00 01 ff fe 7b 00 00" +
                "bc 43 41 91";
        testWriteValue(rowsHexString, "123");
    }

    // insert into drc1.int24(id) values(16777215);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "37 fc 85 62   1e   ea 0c 00 00   27 00 00 00   54 04 00 00   00 00" +
                "83 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "42 f7 45 50";
        testWriteValue(rowsHexString, "16777215");
    }

    // insert into drc1.int24(id) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "f4 fc 85 62   1e   ea 0c 00 00   27 00 00 00   58 05 00 00   00 00" +
                "83 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "21 e6 c9 57";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`int24` (
     *   `id` mediumint unsigned NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='int24';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "bc fb 85 62   13   ea 0c 00 00   30 00 00 00   29 03 00 00   00 00" +
                "83 00 00 00 00 00 01 00  04 64 72 63 31 00 05 69" +
                "6e 74 32 34 00 01 09 00  00 fc 0c 40 07";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "int24";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "mediumint", null, null, null, null, null, null, "mediumint unsigned", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
