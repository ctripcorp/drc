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
//[0, 1099511627775]
public class WriteFieldBitMeta5TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.bit(bit_column) values(12);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "69 33 86 62   1e   ea 0c 00 00   29 00 00 00   03 18 00 00   00 00" +
                "89 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 0c e8 9d 9d da";
        testWriteValue(rowsHexString, "12");
    }

    // insert into drc1.bit(bit_column) values(1099511627775);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "bf 33 86 62   1e   ea 0c 00 00   29 00 00 00   05 19 00 00   00 00" +
                "89 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff ff 8a 31 c4 ce";
        testWriteValue(rowsHexString, "1099511627775");
    }

    // insert into drc1.bit(bit_column) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "e4 33 86 62   1e   ea 0c 00 00   29 00 00 00   07 1a 00 00   00 00" +
                "89 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 c1 99 be c2";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`bit` (
     *   `bit_column` bit(40) NOT NULL COMMENT 'bit位'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bit测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "69 33 86 62   13   ea 0c 00 00   30 00 00 00   da 17 00 00   00 00" +
                "89 00 00 00 00 00 01 00  04 64 72 63 31 00 03 62" +
                "69 74 00 01 10 02 00 05  00 90 65 68 3f";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "bit";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("bit_column", false, "bit", null, "40", null, null, null, null, "bit(40)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
