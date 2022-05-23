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
public class WriteFieldTimestamp2Meta4TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.timestamp4(date) values('1970-01-01 08:00:01.0000');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "70 0d 8a 62   1e   ea 0c 00 00   2a 00 00 00   3d 16 00 00   00 00" +
                "a4 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "01 00 00 b2 63 8b a1";
        testWriteValue(rowsHexString, "1970-01-01 08:00:01.0000");
    }

    // insert into drc1.timestamp4(date) values('2038-01-19 11:14:07.9999');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "cc 0d 8a 62   1e   ea 0c 00 00   2a 00 00 00   4e 17 00 00   00 00" +
                "a4 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff 27 0f 4e 51 d5 19";
        testWriteValue(rowsHexString, "2038-01-19 11:14:07.9999");
    }

    // insert into drc1.timestamp4(date) values('2022-05-20 17:22:54.1234');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "f2 0d 8a 62   1e   ea 0c 00 00   2a 00 00 00   5f 18 00 00   00 00" +
                "a4 00 00 00 00 00 01 00  02 00 01 ff fe 62 87 5d" +
                "ee 04 d2 c3 7f ec e4";
        testWriteValue(rowsHexString, "2022-05-20 17:22:54.1234");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`timestamp4` (
     *   `date` timestamp4(0) NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='timestamp4测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "70 0d 8a 62   13   ea 0c 00 00   36 00 00 00   13 16 00 00   00 00" +
                "a4 00 00 00 00 00 01 00  04 64 72 63 31 00 0a 74" +
                "69 6d 65 73 74 61 6d 70  34 00 01 11 01 04 00 d6" +
                "08 fd 27";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "timestamp4";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "timestamp", null, null, null, "4", null, null, "timestamp(4)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
