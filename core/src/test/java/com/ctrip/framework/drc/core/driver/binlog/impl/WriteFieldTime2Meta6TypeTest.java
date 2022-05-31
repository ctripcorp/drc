package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/18
 */
// document show range '-838:59:59.000000' to '838:59:59.000000'
public class WriteFieldTime2Meta6TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.time2(start_at) values('12:13:14.567');
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "b5 ce 83 62   1e   ea 0c 00 00   2a 00 00 00   bb 24 00 00   00 00" +
                "70 00 00 00 00 00 01 00  02 00 01 ff fe 80 c3 4e" +
                "08 a6 d8 d1 2a 14 2d";
        testWriteValue(rowsHexString, "12:13:14.567000");
    }

    // insert into drc1.time2(start_at) values('-12:13:14.567');
    @Test
    public void testNegative() throws IOException {

        String rowsHexString = "8e cf 83 62   1e   ea 0c 00 00   2a 00 00 00   bf 25 00 00   00 00" +
                "70 00 00 00 00 00 01 00  02 00 01 ff fe 7f 3c b1" +
                "f7 59 28 27 aa f7 48";
        testWriteValue(rowsHexString, "-12:13:14.567000");
    }

    // insert into drc1.time2(start_at) values('838:59:59.000000');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "35 48 84 62   1e   ea 0c 00 00   2a 00 00 00   c3 26 00 00   00 00" +
                "70 00 00 00 00 00 01 00  02 00 01 ff fe b4 6e fb" +
                "00 00 00 4b 0b 34 2a";
        testWriteValue(rowsHexString, "838:59:59.000000");
    }

    // insert into drc1.time2(start_at) values('-838:59:59.000000');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "76 48 84 62   1e   ea 0c 00 00   2a 00 00 00   c7 27 00 00   00 00" +
                "70 00 00 00 00 00 01 00  02 00 01 ff fe 4b 91 05" +
                "00 00 00 53 ee 05 ee";
        testWriteValue(rowsHexString, "-838:59:59.000000");
    }

    // insert into drc1.time2(start_at) values('00:00:00.000000');
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "90 4d 84 62   1e   ea 0c 00 00   2a 00 00 00   d7 2b 00 00   00 00" +
                "70 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 00 14 95 71 7e";
        testWriteValue(rowsHexString, "00:00:00.000000");
    }

    // insert into drc1.time2(start_at) values('00:00:00.010203');
    @Test
    public void testPositiveMillSecond() throws IOException {
        String rowsHexString = "33 49 84 62   1e   ea 0c 00 00   2a 00 00 00   cb 28 00 00   00 00" +
                "70 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 27 db cb 45 65 5c";
        testWriteValue(rowsHexString, "00:00:00.010203");
    }

    // insert into drc1.time2(start_at) values('-00:00:00.010203');
    @Test
    public void testNegativeMillSecond1() throws IOException {
        String rowsHexString = "ad 49 84 62   1e   ea 0c 00 00   2a 00 00 00   cf 29 00 00   00 00" +
                "70 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff d8 25 b8 97 22 95";
        testWriteValue(rowsHexString, "-00:00:00.010203");
    }

    // insert into drc1.time2(start_at) values('-00:00:01.101213');
    @Test
    public void testNegativeMillSecond2() throws IOException {
        String rowsHexString = "ed 4c 84 62   1e   ea 0c 00 00   2a 00 00 00   d3 2a 00 00   00 00" +
                "70 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff fe" +
                "fe 74 a3 c7 9e 71 a8";
        testWriteValue(rowsHexString, "-00:00:01.101213");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`time2` (
     *   `start_at` TIME(6) NOT NULL COMMENT '时间'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='time2测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "b5 ce 83 62   13   ea 0c 00 00   31 00 00 00   91 24 00 00   00 00" +
                "70 00 00 00 00 00 01 00  04 64 72 63 31 00 05 74" +
                "69 6d 65 32 00 01 13 01  06 00 49 ca cd 9e";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "time2";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("start_at", false, "time", null, null, null, "6", null, null, "time(6)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
