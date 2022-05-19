package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// 2bytes
// [-2^15, 2^15-1], [-32,768, 32,767]
// unsigned [0, 2^16 - 1], [0, 65535]
// mysql literal is smallint
public class WriteFieldShortTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.short(id) values(123);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "f4 f5 85 62   1e   ea 0c 00 00   26 00 00 00   4e 1c 00 00   00 00" +
                "80 00 00 00 00 00 01 00  02 00 01 ff fe 7b 00 c9" +
                "6e b0 29";
        testWriteValue(rowsHexString, "123");
    }

    // insert into drc1.short(id) values(-123);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "7b f6 85 62   1e   ea 0c 00 00   26 00 00 00   51 1d 00 00   00 00" +
                "80 00 00 00 00 00 01 00  02 00 01 ff fe 85 ff cc" +
                "a9 21 41";
        testWriteValue(rowsHexString, "-123");
    }

    // insert into drc1.short(id) values(32767);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "26 f7 85 62   1e   ea 0c 00 00   26 00 00 00   57 1f 00 00   00 00" +
                "80 00 00 00 00 00 01 00  02 00 01 ff fe ff 7f 19" +
                "bd ce 89";
        testWriteValue(rowsHexString, "32767");
    }

    // insert into drc1.short(id) values(-32768);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "d9 f6 85 62   1e   ea 0c 00 00   26 00 00 00   54 1e 00 00   00 00" +
                "80 00 00 00 00 00 01 00  02 00 01 ff fe 00 80 f3" +
                "39 4c 11";
        testWriteValue(rowsHexString, "-32768");
    }

    // insert into drc1.short(id) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "80 f2 85 62   1e   ea 0c 00 00   25 00 00 00   5c 14 00 00   00 00" +
                "7e 00 00 00 00 00 01 00  02 00 01 ff fe 00 4c b8" +
                "97 7b";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`short` (
     *   `id` smallint NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='short测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "f4 f5 85 62   13   ea 0c 00 00   30 00 00 00   28 1c 00 00   00 00" +
                "80 00 00 00 00 00 01 00  04 64 72 63 31 00 05 73" +
                "68 6f 72 74 00 01 02 00  00 78 7b 36 f8";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "short";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "smallint", null, null, null, null, null, null, "smallint", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
