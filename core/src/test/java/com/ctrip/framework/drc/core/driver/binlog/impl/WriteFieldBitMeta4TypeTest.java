package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// bit(M), M=[1-64], default M = 1
// 1-8bytes
// big-endian, unsigned
//[0, 4294967295]
public class WriteFieldBitMeta4TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.bit(bit_column) values(12);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "4f 32 86 62   1e   ea 0c 00 00   28 00 00 00   2a 13 00 00   00 00" +
                "88 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "0c 8f d8 6a 14";
        testWriteValue(rowsHexString, "12");
    }

    // insert into drc1.bit(bit_column) values(4294967295);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "b3 32 86 62   1e   ea 0c 00 00   28 00 00 00   2b 14 00 00   00 00" +
                "88 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff d9 a7 a7 d9";
        testWriteValue(rowsHexString, "4294967295");
    }

    // insert into drc1.bit(bit_column) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "e4 32 86 62   1e   ea 0c 00 00   28 00 00 00   2c 15 00 00   00 00" +
                "88 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 93 bf 9b e8";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`bit` (
     *   `bit_column` bit(32) NOT NULL COMMENT 'bit位'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bit测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "4f 32 86 62   13   ea 0c 00 00   30 00 00 00   02 13 00 00   00 00" +
                "88 00 00 00 00 00 01 00  04 64 72 63 31 00 03 62" +
                "69 74 00 01 10 02 00 04  00 11 b2 a1 04";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "bit";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("bit_column", false, "bit", null, "32", null, null, null, null, "bit(32)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
