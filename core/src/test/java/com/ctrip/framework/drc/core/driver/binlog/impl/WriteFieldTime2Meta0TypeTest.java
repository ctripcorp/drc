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
public class WriteFieldTime2Meta0TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.time2(start_at) values('12:13:15');
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "56 4f 84 62   1e   ea 0c 00 00   28 00 00 00   40 32 00 00   00 00" +
                "72 00 00 00 00 00 01 00  02 00 01 ff fe 80 c3 4e" +
                "38 61 e2 75 48";
        testWriteValue(rowsHexString, "12:13:14.56");
    }

    // insert into drc1.time2(start_at) values('-12:13:14.56');
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "88 53 84 62   1e   ea 0c 00 00   28 00 00 00   42 33 00 00   00 00" +
                "72 00 00 00 00 00 01 00  02 00 01 ff fe 7f 3c b1" +
                "c8 4e 74 e3 fd";
        testWriteValue(rowsHexString, "-12:13:14.56");
    }

    // insert into drc1.time2(start_at) values('838:59:59.00');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "b2 53 84 62   1e   ea 0c 00 00   28 00 00 00   44 34 00 00   00 00" +
                "72 00 00 00 00 00 01 00  02 00 01 ff fe b4 6e fb" +
                "00 40 f8 2f 5d";
        testWriteValue(rowsHexString, "838:59:59.00");
    }

    // insert into drc1.time2(start_at) values('-838:59:59.00');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "de 53 84 62   1e   ea 0c 00 00   28 00 00 00   46 35 00 00   00 00" +
                "72 00 00 00 00 00 01 00  02 00 01 ff fe 4b 91 05" +
                "00 f4 1f d7 2f";
        testWriteValue(rowsHexString, "-838:59:59.00");
    }

    // insert into drc1.time2(start_at) values('00:00:00.00');
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "04 54 84 62   1e   ea 0c 00 00   28 00 00 00   48 36 00 00   00 00" +
                "72 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 d9 53 26 ec";
        testWriteValue(rowsHexString, "00:00:00.00");
    }

    // insert into drc1.time2(start_at) values('00:00:00.12');
    @Test
    public void testPositiveMillSecond() throws IOException {
        String rowsHexString = "28 54 84 62   1e   ea 0c 00 00   28 00 00 00   4a 37 00 00   00 00" +
                "72 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "0c 9b 83 2d 4e";
        testWriteValue(rowsHexString, "00:00:00.12");
    }

    // insert into drc1.time3(start_at) values('-00:00:00.12');
    @Test
    public void testNegativeMillSecond1() throws IOException {
        String rowsHexString = "43 57 84 62   1e   ea 0c 00 00   28 00 00 00   4e 39 00 00   00 00" +
                "72 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "f4 0a 58 9b 70";
        testWriteValue(rowsHexString, "-00:00:00.12");
    }

    // insert into drc1.time3(start_at) values('-00:00:01.01');
    @Test
    public void testNegativeMillSecond2() throws IOException {
        String rowsHexString = "57 64 84 62   1e   ea 0c 00 00   28 00 00 00   50 3a 00 00   00 00" +
                "72 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff fe" +
                "ff 9c 62 e7 a9";
        testWriteValue(rowsHexString, "-00:00:01.01");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`time2` (
     *   `start_at` TIME(0) NOT NULL COMMENT '时间'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='time2测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "81 89 84 62   13   ea 0c 00 00   31 00 00 00   6f 02 00 00   00 00" +
                "75 00 00 00 00 00 01 00  04 64 72 63 31 00 05 74" +
                "69 6d 65 32 00 01 13 01  00 00 1c 71 30 cd";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "time2";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("start_at", false, "time", null, null, null, "0", null, null, "time(0)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
