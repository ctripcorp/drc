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
public class WriteFieldShortUnsignedTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.short(id) values(123);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "c3 f7 85 62   1e   ea 0c 00 00   26 00 00 00   e6 22 00 00   00 00" +
                "81 00 00 00 00 00 01 00  02 00 01 ff fe 7b 00 e4" +
                "08 b0 0f";
        testWriteValue(rowsHexString, "123");
    }

    // insert into drc1.short(id) values(65535);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "9c f8 85 62   1e   ea 0c 00 00   26 00 00 00   e9 23 00 00   00 00" +
                "81 00 00 00 00 00 01 00  02 00 01 ff fe ff ff 12" +
                "3a 95 0b";
        testWriteValue(rowsHexString, "65535");
    }

    // insert into drc1.short(id) values(0);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "bd f8 85 62   1e   ea 0c 00 00   26 00 00 00   ec 24 00 00   00 00" +
                "81 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 2b" +
                "6b 3a b6";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`short` (
     *   `id` smallint unsigned NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='short测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "c3 f7 85 62   13   ea 0c 00 00   30 00 00 00   c0 22 00 00   00 00" +
                "81 00 00 00 00 00 01 00  04 64 72 63 31 00 05 73" +
                "68 6f 72 74 00 01 02 00  00 52 3f 99 ce";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "short";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "smallint", null, null, null, null, null, null, "smallint unsigned", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
