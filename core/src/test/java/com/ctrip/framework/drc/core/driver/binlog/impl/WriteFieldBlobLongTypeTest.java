package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldBlobLongTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.longblob(id) values('helloworld123456789,');
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "31 f4 88 62   1e   ea 0c 00 00   3c 00 00 00   4e 12 00 00   00 00" +
                "96 00 00 00 00 00 01 00  02 00 01 ff fe 14 00 00" +
                "00 68 65 6c 6c 6f 77 6f  72 6c 64 31 32 33 34 35" +
                "36 37 38 39 2c f4 f6 64  56";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.longblob(id) values('helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,helloworld123456789,');
    @Test
    public void testValue2() throws IOException {
        String rowsHexString = "97 f4 88 62   1e   ea 0c 00 00   2c 01 00 00   57 14 00 00   00 00" +
                "96 00 00 00 00 00 01 00  02 00 01 ff fe 04 01 00" +
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
                "36 37 38 39 2c 4e 81 f5  c2";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`longblob` (
     *   `id` longblob NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='longblob测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "31 f4 88 62   13   ea 0c 00 00   34 00 00 00   12 12 00 00   00 00" +
                "96 00 00 00 00 00 01 00  04 64 72 63 31 00 08 6c" +
                "6f 6e 67 62 6c 6f 62 00  01 fc 01 04 00 e1 9b 2d" +
                "da";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "longblob";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "longblob", null, null, null, null, null, null, "longblob", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
