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
//[0, 281474976710655]
public class WriteFieldBitMeta6TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.bit(bit_column) values(12);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "55 34 86 62   1e   ea 0c 00 00   2a 00 00 00   df 1c 00 00   00 00" +
                "8a 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 0c 7b 41 b1 f2";
        testWriteValue(rowsHexString, "12");
    }

    // insert into drc1.bit(bit_column) values(281474976710655);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "da 34 86 62   1e   ea 0c 00 00   2a 00 00 00   e2 1d 00 00   00 00" +
                "8a 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff ff ff 31 8b c4 1c";
        testWriteValue(rowsHexString, "281474976710655");
    }

    // insert into drc1.bit(bit_column) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "05 35 86 62   1e   ea 0c 00 00   2a 00 00 00   e5 1e 00 00   00 00" +
                "8a 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 02 3e 72 f8";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`bit` (
     *   `bit_column` bit(48) NOT NULL COMMENT 'bit位'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bit测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "55 34 86 62   13   ea 0c 00 00   30 00 00 00   b5 1c 00 00   00 00" +
                "8a 00 00 00 00 00 01 00  04 64 72 63 31 00 03 62" +
                "69 74 00 01 10 02 00 06  00 18 32 b9 ac";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "bit";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("bit_column", false, "bit", null, "48", null, null, null, null, "bit(48)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
