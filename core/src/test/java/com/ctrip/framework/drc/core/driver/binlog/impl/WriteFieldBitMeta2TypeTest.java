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
//[0, 65535]
public class WriteFieldBitMeta2TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.bit(bit_column) values(12);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "28 30 86 62   1e   ea 0c 00 00   26 00 00 00   81 09 00 00   00 00" +
                "86 00 00 00 00 00 01 00  02 00 01 ff fe 00 0c 2b" +
                "d6 fd 2a";
        testWriteValue(rowsHexString, "12");
    }

    // insert into drc1.bit(bit_column) values(65535);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "9d 30 86 62   1e   ea 0c 00 00   26 00 00 00   80 0a 00 00   00 00" +
                "86 00 00 00 00 00 01 00  02 00 01 ff fe ff ff f9" +
                "b2 8e 83";
        testWriteValue(rowsHexString, "65535");
    }

    // insert into drc1.bit(bit_column) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "bf 30 86 62   1e   ea 0c 00 00   26 00 00 00   7f 0b 00 00   00 00" +
                "86 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 22" +
                "62 cc 0c";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`bit` (
     *   `bit_column` bit(16) NOT NULL COMMENT 'bit位'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bit测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "28 30 86 62   13   ea 0c 00 00   30 00 00 00   5b 09 00 00   00 00" +
                "86 00 00 00 00 00 01 00  04 64 72 63 31 00 03 62" +
                "69 74 00 01 10 02 00 02  00 4f 71 c1 2a";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "bit";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("bit_column", false, "bit", null, "16", null, null, null, null, "bit(16)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
