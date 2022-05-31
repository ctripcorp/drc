package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/22
 */
// document show range : '1000-01-01' to '9999-12-31'
// real range : '0000-01-01' to '9999-12-31'
public class WriteFieldDateTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.date(date) values('1000-01-01');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "57 1b 8a 62   1e   ea 0c 00 00   27 00 00 00   3c 0a 00 00   00 00" +
                "a7 00 00 00 00 00 01 00  02 00 01 ff fe 21 d0 07" +
                "9f c4 d5 de";
        testWriteValue(rowsHexString, "1000-01-01");
    }

    // insert into drc1.date(date) values('0000-01-01');
    @Test
    public void testRealMin() throws IOException {
        String rowsHexString = "25 23 8a 62   1e   ea 0c 00 00   27 00 00 00   3b 0b 00 00   00 00" +
                "a7 00 00 00 00 00 01 00  02 00 01 ff fe 21 00 00" +
                "88 7e e8 da";
        testWriteValue(rowsHexString, "0000-01-01");
    }

    // insert into drc1.date(date) values('9999-12-31');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "5c 23 8a 62   1e   ea 0c 00 00   27 00 00 00   3a 0c 00 00   00 00" +
                "a7 00 00 00 00 00 01 00  02 00 01 ff fe 9f 1f 4e" +
                "7e 40 94 06";
        testWriteValue(rowsHexString, "9999-12-31");
    }

    // insert into drc1.date(date) values('2022-05-20');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "af 23 8a 62   1e   ea 0c 00 00   27 00 00 00   39 0d 00 00   00 00" +
                "a7 00 00 00 00 00 01 00  02 00 01 ff fe b4 cc 0f" +
                "49 ed e4 92";
        testWriteValue(rowsHexString, "2022-05-20");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`date` (
     *   `date` date NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='date';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "57 1b 8a 62   13   ea 0c 00 00   2f 00 00 00   15 0a 00 00   00 00" +
                "a7 00 00 00 00 00 01 00  04 64 72 63 31 00 04 64" +
                "61 74 65 00 01 0a 00 00  9c b8 04 4f";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "date";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "date", null, null, null, null, null, null, "date", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
