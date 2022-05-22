package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldBlobMediumTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.mediumblob(id) values('helloworld123456789,');
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "11 f3 88 62   1e   ea 0c 00 00   3b 00 00 00   14 0e 00 00   00 00" +
                "95 00 00 00 00 00 01 00  02 00 01 ff fe 14 00 00" +
                "68 65 6c 6c 6f 77 6f 72  6c 64 31 32 33 34 35 36" +
                "37 38 39 2c 53 4f 47 2b";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.mediumblob(id) values('helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,');
    @Test
    public void testValue2() throws IOException {
        String rowsHexString = "60 f3 88 62   1e   ea 0c 00 00   2b 01 00 00   1e 10 00 00   00 00" +
                "95 00 00 00 00 00 01 00  02 00 01 ff fe 04 01 00" +
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
                "37 38 39 2c cd a7 7c 8a";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`mediumblob` (
     *   `id` mediumblob NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='mediumblob测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "11 f3 88 62   13   ea 0c 00 00   36 00 00 00   d9 0d 00 00   00 00" +
                "95 00 00 00 00 00 01 00  04 64 72 63 31 00 0a 6d" +
                "65 64 69 75 6d 62 6c 6f  62 00 01 fc 01 03 00 13" +
                "1f 72 89";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "mediumblob";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "mediumblob", null, null, null, null, null, null, "mediumblob", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
