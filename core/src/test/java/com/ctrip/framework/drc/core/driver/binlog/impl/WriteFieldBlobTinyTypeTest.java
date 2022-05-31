package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldBlobTinyTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.tinyblob(id) values('helloworld123456789,');
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "b3 ee 88 62   1e   ea 0c 00 00   39 00 00 00   ca 05 00 00   00 00" +
                "93 00 00 00 00 00 01 00  02 00 01 ff fe 14 68 65" +
                "6c 6c 6f 77 6f 72 6c 64  31 32 33 34 35 36 37 38" +
                "39 2c 87 34 3b 36";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.tinyblob(id) values('helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,');
    @Test
    public void testValue2() throws IOException {
        String rowsHexString = "7a ef 88 62   1e   ea 0c 00 00   15 01 00 00   bc 07 00 00   00 00" +
                "93 00 00 00 00 00 01 00  02 00 01 ff fe f0 68 65" +
                "6c 6c 6f 77 6f 72 6c 64  31 32 33 34 35 36 37 38" +
                "39 2c 68 65 6c 6c 6f 77  6f 72 6c 64 31 32 33 34" +
                "35 36 37 38 39 2c 68 65  6c 6c 6f 77 6f 72 6c 64" +
                "31 32 33 34 35 36 37 38  39 2c 68 65 6c 6c 6f 77" +
                "6f 72 6c 64 31 32 33 34  35 36 37 38 39 2c 68 65" +
                "6c 6c 6f 77 6f 72 6c 64  31 32 33 34 35 36 37 38" +
                "39 2c 68 65 6c 6c 6f 77  6f 72 6c 64 31 32 33 34" +
                "35 36 37 38 39 2c 68 65  6c 6c 6f 77 6f 72 6c 64" +
                "31 32 33 34 35 36 37 38  39 2c 68 65 6c 6c 6f 77" +
                "6f 72 6c 64 31 32 33 34  35 36 37 38 39 2c 68 65" +
                "6c 6c 6f 77 6f 72 6c 64  31 32 33 34 35 36 37 38" +
                "39 2c 68 65 6c 6c 6f 77  6f 72 6c 64 31 32 33 34" +
                "35 36 37 38 39 2c 68 65  6c 6c 6f 77 6f 72 6c 64" +
                "31 32 33 34 35 36 37 38  39 2c 68 65 6c 6c 6f 77" +
                "6f 72 6c 64 31 32 33 34  35 36 37 38 39 2c e2 17" +
                "15 8c";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`tinyblob` (
     *   `id` tinyblob NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='tinyblob测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "b3 ee 88 62   13   ea 0c 00 00   34 00 00 00   91 05 00 00   00 00" +
                "93 00 00 00 00 00 01 00  04 64 72 63 31 00 08 74" +
                "69 6e 79 62 6c 6f 62 00  01 fc 01 01 00 e7 49 8c" +
                "75";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "tinyblob";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "tinyblob", null, null, null, null, null, null, "tinyblob", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
