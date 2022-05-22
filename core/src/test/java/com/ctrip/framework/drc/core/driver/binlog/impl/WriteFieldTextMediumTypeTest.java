package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldTextMediumTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.mediumtext(id) values('helloworld123456789,');
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "5f fc 88 62   1e   ea 0c 00 00   3b 00 00 00   af 02 00 00   00 00" +
                "99 00 00 00 00 00 01 00  02 00 01 ff fe 14 00 00" +
                "68 65 6c 6c 6f 77 6f 72  6c 64 31 32 33 34 35 36" +
                "37 38 39 2c b8 f7 db 26";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.mediumtext(id) values('helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,');
    @Test
    public void testValue2() throws IOException {
        String rowsHexString = "c1 fc 88 62   1e   ea 0c 00 00   2b 01 00 00   b9 04 00 00   00 00" +
                "99 00 00 00 00 00 01 00  02 00 01 ff fe 04 01 00" +
                "68 65 6c 6c 6f 77 6f 72  6c 64 31 32 33 34 35 36" +
                "37 38 39 2c 68 65 6c 6c  6f 77 6f 72 6c 64 31 32" +
                "33 34 35 36 37 38 39 2c  68 65 6c 6c 6f 77 6f 72" +
                "6c 64 31 32 33 34 35 36  37 38 39 2c 68 65 6c 6c" +
                "6f 77 6f 72 6c 64 31 32  33 34 35 36 37 38 39 2c" +
                "68 65 6c 6c 6f 77 6f 72  6c 64 31 32 33 34 35 36" +
                "37 38 39 2c 68 65 6c 6c  6f 77 6f 72 6c 64 31 32" +
                "33 34 35 36 37 38 39 2c  68 65 6c 6c 6f 77 6f 72" +
                "6c 64 31 32 33 34 35 36  37 38 39 2c 68 65 6c 6c" +
                "6f 77 6f 72 6c 64 31 32  33 34 35 36 37 38 39 2c" +
                "68 65 6c 6c 6f 77 6f 72  6c 64 31 32 33 34 35 36" +
                "37 38 39 2c 68 65 6c 6c  6f 77 6f 72 6c 64 31 32" +
                "33 34 35 36 37 38 39 2c  68 65 6c 6c 6f 77 6f 72" +
                "6c 64 31 32 33 34 35 36  37 38 39 2c 68 65 6c 6c" +
                "6f 77 6f 72 6c 64 31 32  33 34 35 36 37 38 39 2c" +
                "68 65 6c 6c 6f 77 6f 72  6c 64 31 32 33 34 35 36" +
                "37 38 39 2c a2 d7 00 31";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`mediumtext` (
     *   `id` mediumtext NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='mediumtext测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "5f fc 88 62   13   ea 0c 00 00   36 00 00 00   74 02 00 00   00 00" +
                "99 00 00 00 00 00 01 00  04 64 72 63 31 00 0a 6d" +
                "65 64 69 75 6d 74 65 78  74 00 01 fc 01 03 00 5f" +
                "e5 09 1b";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "mediumtext";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "mediumtext", null, null, null, null, null, null, "mediumtext", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
