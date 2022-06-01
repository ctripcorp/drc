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
//[0, 255]
public class WriteFieldBitMeta1TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.bit(bit_column) values(12);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "65 2d 86 62   1e   ea 0c 00 00   25 00 00 00   b1 04 00 00   00 00" +
                "85 00 00 00 00 00 01 00  02 00 01 ff fe 0c 89 71" +
                "41 53";
        testWriteValue(rowsHexString, "12");
    }

    // insert into drc1.bit(bit_column) values(255);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "a1 2e 86 62   1e   ea 0c 00 00   25 00 00 00   af 05 00 00   00 00" +
                "85 00 00 00 00 00 01 00  02 00 01 ff fe ff d9 f9" +
                "df e2";
        testWriteValue(rowsHexString, "255");
    }

    // insert into drc1.bit(bit_column) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "5c 2f 86 62   1e   ea 0c 00 00   25 00 00 00   ad 06 00 00   00 00" +
                "85 00 00 00 00 00 01 00  02 00 01 ff fe 00 35 b3" +
                "4d ce";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`bit` (
     *   `bit_column` bit(8) NOT NULL COMMENT 'bit位'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bit测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "65 2d 86 62   13   ea 0c 00 00   30 00 00 00   8c 04 00 00   00 00" +
                "85 00 00 00 00 00 01 00  04 64 72 63 31 00 03 62" +
                "69 74 00 01 10 02 00 01  00 59 74 55 eb";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "bit";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("bit_column", false, "bit", null, "8", null, null, null, null, "bit(8)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
