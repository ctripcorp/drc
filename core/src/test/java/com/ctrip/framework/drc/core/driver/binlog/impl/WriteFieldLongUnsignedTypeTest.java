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
// [-2^31, 2^31 - 1], [-2,147,483,648, 2,147,483,647]
// unsigned [0, 2^32 - 1], [0, 4294967295]
// mysql literal is int
public class WriteFieldLongUnsignedTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.long(id) values(12);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "70 9d 84 62   1e   ea 0c 00 00   28 00 00 00   db 12 00 00   00 00" +
                "77 00 00 00 00 00 01 00  02 00 01 ff fe 0c 00 00" +
                "00 ff ce 3d 17";
        testWriteValue(rowsHexString, "12");
    }

    // insert into drc1.long(id) values(4294967295);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "8b 9f 84 62   1e   ea 0c 00 00   28 00 00 00   db 13 00 00   00 00" +
                "77 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff d6 f8 ef 68";
        testWriteValue(rowsHexString, "4294967295");
    }

    // insert into drc1.long(id) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "08 a0 84 62   1e   ea 0c 00 00   28 00 00 00   db 14 00 00   00 00" +
                "77 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 0c 96 a0 c9";
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
        return "70 9d 84 62   13   ea 0c 00 00   2f 00 00 00   b3 12 00 00   00 00" +
                "77 00 00 00 00 00 01 00  04 64 72 63 31 00 04 6c" +
                "6f 6e 67 00 01 03 00 00  43 2b f5 5d";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "long";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "int", null, null, null, null, null, null, "int unsigned", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
