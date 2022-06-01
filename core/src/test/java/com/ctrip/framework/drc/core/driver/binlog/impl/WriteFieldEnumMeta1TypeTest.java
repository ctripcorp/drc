package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/22
 */
public class WriteFieldEnumMeta1TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.enum(size) value('small')
    @Test
    public void testValue1() throws IOException {
        String rowsHexString = "5e 33 8a 62   1e   ea 0c 00 00   25 00 00 00   7a 01 00 00   00 00" +
                "a8 00 00 00 00 00 01 00  02 00 01 ff fe 02 e0 38" +
                "ea e1";
        testWriteValue(rowsHexString, "2");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`enum` (
     *   `size` enum('x-small', 'small', 'medium', 'large', 'x-large')
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='enum测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "5e 33 8a 62   13   ea 0c 00 00   31 00 00 00   55 01 00 00   00 00" +
                "a8 00 00 00 00 00 01 00  04 64 72 63 31 00 04 65" +
                "6e 75 6d 00 01 fe 02 f7  01 01 0a 67 0f d0";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "enum";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("size", false, "enum", null, null, null, null, null, null, "enum('x-small','small','medium','large','x-large')", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
