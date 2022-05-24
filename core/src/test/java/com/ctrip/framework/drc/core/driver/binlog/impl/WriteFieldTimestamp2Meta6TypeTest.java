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
public class WriteFieldTimestamp2Meta6TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.timestamp6(date) values('1970-01-01 08:00:01.000000');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "28 0e 8a 62   1e   ea 0c 00 00   2b 00 00 00   71 19 00 00   00 00" +
                "a5 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "01 00 00 00 fc 99 a8 5e";
        testWriteValue(rowsHexString, "1970-01-01 08:00:01.000000");
    }

    // insert into drc1.timestamp6(date) values('2038-01-19 11:14:07.999999');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "7d 0e 8a 62   1e   ea 0c 00 00   2b 00 00 00   83 1a 00 00   00 00" +
                "a5 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff 0f 42 3f be fa 6b b2";
        testWriteValue(rowsHexString, "2038-01-19 11:14:07.999999");
    }

    // insert into drc1.timestamp6(date) values('2022-05-20 17:22:54.123456');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "9d 0e 8a 62   1e   ea 0c 00 00   2b 00 00 00   95 1b 00 00   00 00" +
                "a5 00 00 00 00 00 01 00  02 00 01 ff fe 62 87 5d" +
                "ee 01 e2 40 1c 03 06 0a";
        testWriteValue(rowsHexString, "2022-05-20 17:22:54.123456");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`timestamp6` (
     *   `date` timestamp6(0) NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='timestamp6测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "28 0e 8a 62   13   ea 0c 00 00   36 00 00 00   46 19 00 00   00 00" +
                "a5 00 00 00 00 00 01 00  04 64 72 63 31 00 0a 74" +
                "69 6d 65 73 74 61 6d 70  36 00 01 11 01 06 00 10" +
                "be 15 59";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "timestamp6";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "timestamp", null, null, null, "6", null, null, "timestamp(6)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
