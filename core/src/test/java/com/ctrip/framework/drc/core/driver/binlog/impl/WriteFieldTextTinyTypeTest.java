package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldTextTinyTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.tinytext(id) values('helloworld123456789,');
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "ae fb 88 62   1e   ea 0c 00 00   39 00 00 00   91 1a 00 00   00 00" +
                "98 00 00 00 00 00 01 00  02 00 01 ff fe 14 68 65" +
                "6c 6c 6f 77 6f 72 6c 64  31 32 33 34 35 36 37 38" +
                "39 2c 2b 84 7c dc";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.tinytext(id) values('helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,');
    @Test
    public void testValue2() throws IOException {
        String rowsHexString = "04 fb 88 62   1e   ea 0c 00 00   15 01 00 00   7b 19 00 00   00 00" +
                "98 00 00 00 00 00 01 00  02 00 01 ff fe f0 68 65" +
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
                "6f 72 6c 64 31 32 33 34  35 36 37 38 39 2c 9d 89" +
                "f2 7b";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`tinytext` (
     *   `id` tinytext NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='tinytext测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "04 fb 88 62   13   ea 0c 00 00   34 00 00 00   66 18 00 00   00 00" +
                "98 00 00 00 00 00 01 00  04 64 72 63 31 00 08 74" +
                "69 6e 79 74 65 78 74 00  01 fc 01 01 00 d2 66 26" +
                "2a";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "tinytext";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "tinytext", null, null, null, null, null, null, "tinytext", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
