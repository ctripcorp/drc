package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// include binary, char
// char存储字符数[0-255], 无论何种字符集; binary没有字符集
// include varbinary, varchar;
// varchar[M], M=[0-65535], 存储字符数取值要看字符集
public class WriteFieldBinaryVarTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.varbinary(id) values('helloworld123456789,');
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "cf e9 88 62   1e   ea 0c 00 00   3a 00 00 00   94 01 00 00   00 00" +
                "92 00 00 00 00 00 01 00  02 00 01 ff fe 14 00 68" +
                "65 6c 6c 6f 77 6f 72 6c  64 31 32 33 34 35 36 37" +
                "38 39 2c 54 4b a0 f0";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.varbinary(id) values('helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,');
    @Test
    public void testValue2() throws IOException {
        String rowsHexString = "2a eb 88 62   1e   ea 0c 00 00   2a 01 00 00   9d 03 00 00   00 00" +
                "92 00 00 00 00 00 01 00  02 00 01 ff fe 04 01 68" +
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
                "38 39 2c 8a 8f 1e 5e";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`varbinary` (
     *   `id` varbinary(300) NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='varbinary测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "cf e9 88 62   13   ea 0c 00 00   36 00 00 00   5a 01 00 00   00 00" +
                "92 00 00 00 00 00 01 00  04 64 72 63 31 00 09 76" +
                "61 72 62 69 6e 61 72 79  00 01 0f 02 2c 01 00 99" +
                "e7 40 35";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "varbinary";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "varbinary", "300", null, null, null, null, null, "varbinary(300)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
