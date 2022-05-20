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
//[0, 7.205759403792793e16]
public class WriteFieldBitMeta7TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.bit(bit_column) values(12);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "98 35 86 62   1e   ea 0c 00 00   2b 00 00 00   be 21 00 00   00 00" +
                "8b 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 0c 68 37 a4 6c";
        testWriteValue(rowsHexString, "12");
    }

    // insert into drc1.bit(bit_column) values(7.205759403792793e16);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "3a 38 86 62   1e   ea 0c 00 00   2b 00 00 00   ca 24 00 00   00 00" +
                "8b 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff ff ff f8 02 e5 46 d1";
        testWriteValue(rowsHexString, "72057594037927928");
    }

    // insert into drc1.bit(bit_column) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "b7 38 86 62   1e   ea 0c 00 00   2b 00 00 00   ce 25 00 00   00 00" +
                "8b 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 00 d4 7a d3 52";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`bit` (
     *   `bit_column` bit(56) NOT NULL COMMENT 'bit位'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bit测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "98 35 86 62   13   ea 0c 00 00   30 00 00 00   93 21 00 00   00 00" +
                "8b 00 00 00 00 00 01 00  04 64 72 63 31 00 03 62" +
                "69 74 00 01 10 02 00 07  00 ec 65 6e 5e";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "bit";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("bit_column", false, "bit", null, "56", null, null, null, null, "bit(56)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
