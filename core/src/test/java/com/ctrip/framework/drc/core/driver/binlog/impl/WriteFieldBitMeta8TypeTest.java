package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/18
 */
// bit(M), M=[1-64], default M = 1
// 1-8bytes
// big-endian, unsigned
public class WriteFieldBitMeta8TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.bit(bit_column) values(5);
    @Test
    public void testLessThanLong63Max() throws IOException {
        String rowsHexString = "7c cd 84 62   1e   ea 0c 00 00   2c 00 00 00   4a 03 00 00   00 00" +
                "7a 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 00 05 f7 af df  8d";
        testWriteValue(rowsHexString, "5");
    }

    // insert into drc1.bit(bit_column) values(9223372036854775809);
    @Test
    public void testMoreThanLong63Max() throws IOException {
        String rowsHexString = "57 d1 84 62   1e   ea 0c 00 00   2c 00 00 00   59 06 00 00   00 00" +
                "7a 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 00 00 01 59 b8 a8  50";
        testWriteValue(rowsHexString, "9223372036854775809");
    }

    // insert into drc1.bit(bit_column) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "05 d3 84 62   1e   ea 0c 00 00   2c 00 00 00   63 08 00 00   00 00" +
                "7a 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 00 00 f6 d5 17  6a";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`bit` (
     *   `bit_column` bit(64) NOT NULL COMMENT 'bit位'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bit测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "7c cd 84 62   13   ea 0c 00 00   30 00 00 00   1e 03 00 00   00 00" +
                "7a 00 00 00 00 00 01 00  04 64 72 63 31 00 03 62" +
                "69 74 00 01 10 02 00 08  00 f5 4b 72 92";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "bit";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("bit_column", false, "bit", null, "64", null, null, null, null, "bit(64)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
