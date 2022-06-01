package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/22
 */
// '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'
// '1970-01-01 08:00:01.000000' to '2038-01-19 11:14:07.999999'
public class WriteFieldTimestamp2Meta0TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.timestamp0(date) values('1970-01-01 08:00:01');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "f0 09 8a 62   1e   ea 0c 00 00   28 00 00 00   b7 0b 00 00   00 00" +
                "a2 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "01 73 38 0f b8";
        testWriteValue(rowsHexString, "1970-01-01 08:00:01");
    }

    // insert into drc1.timestamp0(date) values('2038-01-19 11:14:07');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "c2 0a 8a 62   1e   ea 0c 00 00   28 00 00 00   c6 0c 00 00   00 00" +
                "a2 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff 9a e8 ec 86";
        testWriteValue(rowsHexString, "2038-01-19 11:14:07");
    }

    // insert into drc1.timestamp0(date) values('2022-05-20 17:22:54');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "e7 0a 8a 62   1e   ea 0c 00 00   28 00 00 00   d5 0d 00 00   00 00" +
                "a2 00 00 00 00 00 01 00  02 00 01 ff fe 62 87 5d" +
                "ee e3 35 90 5c";
        testWriteValue(rowsHexString, "2022-05-20 17:22:54");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`timestamp0` (
     *   `date` timestamp0(0) NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='timestamp0测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "f0 09 8a 62   13   ea 0c 00 00   36 00 00 00   8f 0b 00 00   00 00" +
                "a2 00 00 00 00 00 01 00  04 64 72 63 31 00 0a 74" +
                "69 6d 65 73 74 61 6d 70  30 00 01 11 01 00 00 67" +
                "a8 61 20";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "timestamp0";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "timestamp", null, null, null, "0", null, null, "timestamp(0)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
