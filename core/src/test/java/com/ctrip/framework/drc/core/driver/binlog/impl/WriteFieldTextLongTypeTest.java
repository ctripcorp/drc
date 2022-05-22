package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldTextLongTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.longtext(id) values('helloworld123456789,');
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "68 fd 88 62   1e   ea 0c 00 00   3c 00 00 00   e6 06 00 00   00 00" +
                "9a 00 00 00 00 00 01 00  02 00 01 ff fe 14 00 00" +
                "00 68 65 6c 6c 6f 77 6f  72 6c 64 31 32 33 34 35" +
                "36 37 38 39 2c 2a 87 f3  86";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.longtext(id) values('helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,');
    @Test
    public void testValue2() throws IOException {
        String rowsHexString = "a1 fd 88 62   1e   ea 0c 00 00   2c 01 00 00   ef 08 00 00   00 00" +
                "9a 00 00 00 00 00 01 00  02 00 01 ff fe 04 01 00" +
                "00 68 65 6c 6c 6f 77 6f  72 6c 64 31 32 33 34 35" +
                "36 37 38 39 2c 68 65 6c  6c 6f 77 6f 72 6c 64 31" +
                "32 33 34 35 36 37 38 39  2c 68 65 6c 6c 6f 77 6f" +
                "72 6c 64 31 32 33 34 35  36 37 38 39 2c 68 65 6c" +
                "6c 6f 77 6f 72 6c 64 31  32 33 34 35 36 37 38 39" +
                "2c 68 65 6c 6c 6f 77 6f  72 6c 64 31 32 33 34 35" +
                "36 37 38 39 2c 68 65 6c  6c 6f 77 6f 72 6c 64 31" +
                "32 33 34 35 36 37 38 39  2c 68 65 6c 6c 6f 77 6f" +
                "72 6c 64 31 32 33 34 35  36 37 38 39 2c 68 65 6c" +
                "6c 6f 77 6f 72 6c 64 31  32 33 34 35 36 37 38 39" +
                "2c 68 65 6c 6c 6f 77 6f  72 6c 64 31 32 33 34 35" +
                "36 37 38 39 2c 68 65 6c  6c 6f 77 6f 72 6c 64 31" +
                "32 33 34 35 36 37 38 39  2c 68 65 6c 6c 6f 77 6f" +
                "72 6c 64 31 32 33 34 35  36 37 38 39 2c 68 65 6c" +
                "6c 6f 77 6f 72 6c 64 31  32 33 34 35 36 37 38 39" +
                "2c 68 65 6c 6c 6f 77 6f  72 6c 64 31 32 33 34 35" +
                "36 37 38 39 2c 42 ad 9d  b0";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`longtext` (
     *   `id` longtext NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='longtext测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "68 fd 88 62   13   ea 0c 00 00   34 00 00 00   aa 06 00 00   00 00" +
                "9a 00 00 00 00 00 01 00  04 64 72 63 31 00 08 6c" +
                "6f 6e 67 74 65 78 74 00  01 fc 01 04 00 21 e0 2b" +
                "f6";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "longtext";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "longtext", null, null, null, null, null, null, "longtext", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
