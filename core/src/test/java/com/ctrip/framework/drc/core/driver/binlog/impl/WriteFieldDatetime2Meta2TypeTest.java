package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/22
 */
// document show range : '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
// real range : '0000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
public class WriteFieldDatetime2Meta2TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.datetime2(date) values('1000-01-01 00:00:00.00');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "a2 02 8a 62   1e   ea 0c 00 00   2a 00 00 00   d8 09 00 00   00 00" +
                "a0 00 00 00 00 00 01 00  02 00 01 ff fe 8c b2 42" +
                "00 00 00 00 d4 f6 94";
        testWriteValue(rowsHexString, "1000-01-01 00:00:00.00");
    }

    // insert into drc1.datetime2(date) values('9999-12-31 23:59:59.99');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "e9 02 8a 62   1e   ea 0c 00 00   2a 00 00 00   e0 0a 00 00   00 00" +
                "a0 00 00 00 00 00 01 00  02 00 01 ff fe fe f3 ff" +
                "7e fb 63 11 bc f6 f8";
        testWriteValue(rowsHexString, "9999-12-31 23:59:59.99");
    }

    // insert into drc1.datetime2(date) values('0000-01-01 00:00:00.00');
    @Test
    public void testRealMin() throws IOException {
        String rowsHexString = "14 03 8a 62   1e   ea 0c 00 00   2a 00 00 00   e8 0b 00 00   00 00" +
                "a0 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 42" +
                "00 00 00 3a 66 77 0b";
        testWriteValue(rowsHexString, "0000-01-01 00:00:00.00");
    }

    // insert into drc1.datetime2(date) values('1234-01-01 12:34:56.12');
    @Test
    public void testSpecificValue1() throws IOException {
        String rowsHexString = "43 03 8a 62   1e   ea 0c 00 00   2a 00 00 00   f0 0c 00 00   00 00" +
                "a0 00 00 00 00 00 01 00  02 00 01 ff fe 8f aa c2" +
                "c8 b8 0c 18 00 4f ca";
        testWriteValue(rowsHexString, "1234-01-01 12:34:56.12");
    }

    // insert into drc1.datetime2(date) values('2022-05-20 17:28:54.19');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "a4 03 8a 62   1e   ea 0c 00 00   2a 00 00 00   f8 0d 00 00   00 00" +
                "a0 00 00 00 00 00 01 00  02 00 01 ff fe 99 ac e9" +
                "17 36 13 c3 52 94 54";
        testWriteValue(rowsHexString, "2022-05-20 17:28:54.19");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`datetime` (
     *   `date` datetime(6) NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='datetime测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "a2 02 8a 62   13   ea 0c 00 00   35 00 00 00   ae 09 00 00   00 00" +
                "a0 00 00 00 00 00 01 00  04 64 72 63 31 00 09 64" +
                "61 74 65 74 69 6d 65 32  00 01 12 01 02 00 d2 6b" +
                "38 85";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "datetime";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "datetime", null, null, null, "2", null, null, "datetime(2)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
