package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// 1byte
// [-2^7，2^7 -1], [-128, 127]
// unsigned [0, 2^8 - 1], [0, 255]
// mysql literal is tinyint
public class WriteFieldTinyTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.tiny(id) values(123);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "06 f1 85 62   1e   ea 0c 00 00   25 00 00 00   58 10 00 00   00 00" +
                "7e 00 00 00 00 00 01 00  02 00 01 ff fe 7b 03 9e" +
                "18 1c";
        testWriteValue(rowsHexString, "123");
    }

    // insert into drc1.tiny(id) values(-123);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "b8 f1 85 62   1e   ea 0c 00 00   25 00 00 00   59 11 00 00   00 00" +
                "7e 00 00 00 00 00 01 00  02 00 01 ff fe 85 b7 19" +
                "93 e7";
        testWriteValue(rowsHexString, "-123");
    }

    // insert into drc1.tiny(id) values(127);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "0f f2 85 62   1e   ea 0c 00 00   25 00 00 00   5a 12 00 00   00 00" +
                "7e 00 00 00 00 00 01 00  02 00 01 ff fe 7f f2 33" +
                "07 36";
        testWriteValue(rowsHexString, "127");
    }

    // insert into drc1.tiny(id) values(-128);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "4e f2 85 62   1e   ea 0c 00 00   25 00 00 00   5b 13 00 00   00 00" +
                "7e 00 00 00 00 00 01 00  02 00 01 ff fe 80 d8 f5" +
                "bc 4e";
        testWriteValue(rowsHexString, "-128");
    }

    // insert into drc1.tiny(id) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "80 f2 85 62   1e   ea 0c 00 00   25 00 00 00   5c 14 00 00   00 00" +
                "7e 00 00 00 00 00 01 00  02 00 01 ff fe 00 4c b8" +
                "97 7b";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`tiny` (
     *   `id` tinyint NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='tiny测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "06 f1 85 62   13   ea 0c 00 00   2f 00 00 00   33 10 00 00   00 00" +
                "7e 00 00 00 00 00 01 00  04 64 72 63 31 00 04 74" +
                "69 6e 79 00 01 01 00 00  39 e2 87 d9";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "tiny";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "tinyint", null, null, null, null, null, null, "tinyint", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
