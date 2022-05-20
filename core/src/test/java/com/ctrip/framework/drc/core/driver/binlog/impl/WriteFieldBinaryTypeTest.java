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
public class WriteFieldBinaryTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.binary(id) values('hello ,  world !');
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "4e 1f 87 62   1e   ea 0c 00 00   35 00 00 00   cd 03 00 00   00 00" +
                "90 00 00 00 00 00 01 00  02 00 01 ff fe 10 68 65" +
                "6c 6c 6f 20 2c 20 20 77  6f 72 6c 64 20 21 5e c5" +
                "c4 1b";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.binary(id) values('hello');
    @Test
    public void testValue2() throws IOException {
        String rowsHexString = "0e 28 87 62   1e   ea 0c 00 00   2a 00 00 00   d3 04 00 00   00 00" +
                "90 00 00 00 00 00 01 00  02 00 01 ff fe 05 68 65" +
                "6c 6c 6f cb ca a5 fe";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`binary` (
     *   `id` binary(17) NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='binary测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "4e 1f 87 62   13   ea 0c 00 00   33 00 00 00   98 03 00 00   00 00" +
                "90 00 00 00 00 00 01 00  04 64 72 63 31 00 06 62" +
                "69 6e 61 72 79 00 01 fe  02 fe 11 00 c6 2a c9 ba";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "binary";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "binary", "128", null, null, null, null, null, "binary(17)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
