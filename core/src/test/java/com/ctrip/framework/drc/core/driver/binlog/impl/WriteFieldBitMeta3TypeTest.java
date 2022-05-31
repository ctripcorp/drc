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
//[0, 16777215]
public class WriteFieldBitMeta3TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.bit(bit_column) values(12);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "49 31 86 62   1e   ea 0c 00 00   27 00 00 00   54 0e 00 00   00 00" +
                "87 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 0c" +
                "ee 5f a8 0c";
        testWriteValue(rowsHexString, "12");
    }

    // insert into drc1.bit(bit_column) values(16777215);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "be 31 86 62   1e   ea 0c 00 00   27 00 00 00   54 0f 00 00   00 00" +
                "87 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "d9 28 e4 bf";
        testWriteValue(rowsHexString, "16777215");
    }

    // insert into drc1.bit(bit_column) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "e3 31 86 62   1e   ea 0c 00 00   27 00 00 00   54 10 00 00   00 00" +
                "87 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "40 25 88 9e";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`bit` (
     *   `bit_column` bit(24) NOT NULL COMMENT 'bit位'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bit测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "49 31 86 62   13   ea 0c 00 00   30 00 00 00   2d 0e 00 00   00 00" +
                "87 00 00 00 00 00 01 00  04 64 72 63 31 00 03 62" +
                "69 74 00 01 10 02 00 03  00 7d 4c 02 41";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "bit";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("bit_column", false, "bit", null, "24", null, null, null, null, "bit(24)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
