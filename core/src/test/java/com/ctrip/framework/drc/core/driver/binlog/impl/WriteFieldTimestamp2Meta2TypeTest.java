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
public class WriteFieldTimestamp2Meta2TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.timestamp2(date) values('1970-01-01 08:00:01.00');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "2c 0c 8a 62   1e   ea 0c 00 00   29 00 00 00   be 10 00 00   00 00" +
                "a3 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "01 00 a0 98 3e 58";
        testWriteValue(rowsHexString, "1970-01-01 08:00:01.00");
    }

    // insert into drc1.timestamp2(date) values('2038-01-19 11:14:07.99');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "a2 0c 8a 62   1e   ea 0c 00 00   29 00 00 00   ce 11 00 00   00 00" +
                "a3 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff 63 81 8a d3 ee";
        testWriteValue(rowsHexString, "2038-01-19 11:14:07.99");
    }

    // insert into drc1.timestamp2(date) values('2022-05-20 17:22:54.00');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "c5 0c 8a 62   1e   ea 0c 00 00   29 00 00 00   de 12 00 00   00 00" +
                "a3 00 00 00 00 00 01 00  02 00 01 ff fe 62 87 5d" +
                "ee 00 2d 29 f2 2f";
        testWriteValue(rowsHexString, "2022-05-20 17:22:54.00");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`timestamp2` (
     *   `date` timestamp2(0) NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='timestamp2测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "2c 0c 8a 62   13   ea 0c 00 00   36 00 00 00   95 10 00 00   00 00" +
                "a3 00 00 00 00 00 01 00  04 64 72 63 31 00 0a 74" +
                "69 6d 65 73 74 61 6d 70  32 00 01 11 01 02 00 38" +
                "83 fd 7e";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "timestamp2";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "timestamp", null, null, null, "2", null, null, "timestamp(2)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
