package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldTextTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.text(id) values('helloworld123456789,');
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "f1 f8 88 62   1e   ea 0c 00 00   3a 00 00 00   75 16 00 00   00 00" +
                "97 00 00 00 00 00 01 00  02 00 01 ff fe 14 00 68" +
                "65 6c 6c 6f 77 6f 72 6c  64 31 32 33 34 35 36 37" +
                "38 39 2c d5 8b ac 36";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.text(id) values('helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,');
    @Test
    public void testValue2() throws IOException {
        String rowsHexString = "af f1 88 62   1e   ea 0c 00 00   2a 01 00 00   dd 0b 00 00   00 00" +
                "94 00 00 00 00 00 01 00  02 00 01 ff fe 04 01 68" +
                "65 6c 6c 6f 77 6f 72 6c  64 31 32 33 34 35 36 37" +
                "38 39 2c 68 65 6c 6c 6f  77 6f 72 6c 64 31 32 33" +
                "34 35 36 37 38 39 2c 68  65 6c 6c 6f 77 6f 72 6c" +
                "64 31 32 33 34 35 36 37  38 39 2c 68 65 6c 6c 6f" +
                "77 6f 72 6c 64 31 32 33  34 35 36 37 38 39 2c 68" +
                "65 6c 6c 6f 77 6f 72 6c  64 31 32 33 34 35 36 37" +
                "38 39 2c 68 65 6c 6c 6f  77 6f 72 6c 64 31 32 33" +
                "34 35 36 37 38 39 2c 68  65 6c 6c 6f 77 6f 72 6c" +
                "64 31 32 33 34 35 36 37  38 39 2c 68 65 6c 6c 6f" +
                "77 6f 72 6c 64 31 32 33  34 35 36 37 38 39 2c 68" +
                "65 6c 6c 6f 77 6f 72 6c  64 31 32 33 34 35 36 37" +
                "38 39 2c 68 65 6c 6c 6f  77 6f 72 6c 64 31 32 33" +
                "34 35 36 37 38 39 2c 68  65 6c 6c 6f 77 6f 72 6c" +
                "64 31 32 33 34 35 36 37  38 39 2c 68 65 6c 6c 6f" +
                "77 6f 72 6c 64 31 32 33  34 35 36 37 38 39 2c 68" +
                "65 6c 6c 6f 77 6f 72 6c  64 31 32 33 34 35 36 37" +
                "38 39 2c 92 2d b9 55";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`text` (
     *   `id` text NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='text测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "f1 f8 88 62   13   ea 0c 00 00   30 00 00 00   3b 16 00 00   00 00" +
                "97 00 00 00 00 00 01 00  04 64 72 63 31 00 04 74" +
                "65 78 74 00 01 fc 01 02  00 69 70 f2 eb";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "text";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "text", null, null, null, null, null, null, "text", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
