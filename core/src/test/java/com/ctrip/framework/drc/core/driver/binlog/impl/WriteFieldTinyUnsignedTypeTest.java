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
public class WriteFieldTinyUnsignedTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.tiny(id) values(123);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "15 f3 85 62   1e   ea 0c 00 00   25 00 00 00   34 18 00 00   00 00" +
                "7f 00 00 00 00 00 01 00  02 00 01 ff fe 7b b4 10" +
                "9d ce";
        testWriteValue(rowsHexString, "123");
    }

    // insert into drc1.tiny(id) values(255);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "11 f4 85 62   1e   ea 0c 00 00   25 00 00 00   35 19 00 00   00 00" +
                "7f 00 00 00 00 00 01 00  02 00 01 ff fe ff ca bd" +
                "92 f5";
        testWriteValue(rowsHexString, "255");
    }

    // insert into drc1.tiny(id) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "61 f4 85 62   1e   ea 0c 00 00   25 00 00 00   36 1a 00 00   00 00" +
                "7f 00 00 00 00 00 01 00  02 00 01 ff fe 00 f9 99" +
                "8d b4";
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
        return "15 f3 85 62   13   ea 0c 00 00   2f 00 00 00   0f 18 00 00   00 00" +
                "7f 00 00 00 00 00 01 00  04 64 72 63 31 00 04 74" +
                "69 6e 79 00 01 01 00 00  3d fa 90 54";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "tiny";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "tinyint", null, null, null, null, null, null, "tinyint unsigned", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
