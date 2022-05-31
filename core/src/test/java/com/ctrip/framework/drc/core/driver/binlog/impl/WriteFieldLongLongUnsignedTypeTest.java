package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// 8bytes
// [-2^63, 2^63 - 1], [-9223372036854775808, 9223372036854775807],
// unsigned [0, 2^64 - 1], [0, 18446744073709551615]
// mysql literal is bigint
public class WriteFieldLongLongUnsignedTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.longlong(id) values(1234);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "34 e7 85 62   1e   ea 0c 00 00   2c 00 00 00   2d 0c 00 00   00 00" +
                "7d 00 00 00 00 00 01 00  02 00 01 ff fe d2 04 00" +
                "00 00 00 00 00 63 04 71  14";
        testWriteValue(rowsHexString, "1234");
    }

    // insert into drc1.longlong(id) values(18446744073709551615);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "bd e8 85 62   1e   ea 0c 00 00   2c 00 00 00   39 0d 00 00   00 00" +
                "7d 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff ff ff ff ff 3a 41 96  0c";
        testWriteValue(rowsHexString, "18446744073709551615");
    }

    // insert into drc1.longlong(id) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "34 e9 85 62   1e   ea 0c 00 00   2c 00 00 00   45 0e 00 00   00 00" +
                "7d 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 00 00 77 59 18  08";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`longlong` (
     *   `id` bigint unsigned NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='longlong测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "34 e7 85 62   13   ea 0c 00 00   33 00 00 00   01 0c 00 00   00 00" +
                "7d 00 00 00 00 00 01 00  04 64 72 63 31 00 08 6c" +
                "6f 6e 67 6c 6f 6e 67 00  01 08 00 00 7d a5 73 d7";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "longlong";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "bigint", null, null, null, null, null, null, "bigint unsigned", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
