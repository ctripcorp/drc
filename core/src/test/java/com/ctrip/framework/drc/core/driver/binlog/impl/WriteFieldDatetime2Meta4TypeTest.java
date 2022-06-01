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
public class WriteFieldDatetime2Meta4TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.datetime4(date) values('1000-01-01 00:00:00.0000');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "64 04 8a 62   1e   ea 0c 00 00   2b 00 00 00   8d 02 00 00   00 00" +
                "a1 00 00 00 00 00 01 00  02 00 01 ff fe 8c b2 42" +
                "00 00 00 00 31 15 ff 5e";
        testWriteValue(rowsHexString, "1000-01-01 00:00:00.0000");
    }

    // insert into drc1.datetime4(date) values('9999-12-31 23:59:59.9999');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "b5 04 8a 62   1e   ea 0c 00 00   2b 00 00 00   96 03 00 00   00 00" +
                "a1 00 00 00 00 00 01 00  02 00 01 ff fe fe f3 ff" +
                "7e fb 27 0f 11 48 d6 2c";
        testWriteValue(rowsHexString, "9999-12-31 23:59:59.9999");
    }

    // insert into drc1.datetime4(date) values('0000-01-01 00:00:00.0000');
    @Test
    public void testRealMin() throws IOException {
        String rowsHexString = "16 05 8a 62   1e   ea 0c 00 00   2b 00 00 00   9f 04 00 00   00 00" +
                "a1 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 42" +
                "00 00 00 00 58 16 05 90";
        testWriteValue(rowsHexString, "0000-01-01 00:00:00.0000");
    }

    // insert into drc1.datetime4(date) values('1234-01-01 12:34:56.1234');
    @Test
    public void testSpecificValue1() throws IOException {
        String rowsHexString = "75 05 8a 62   1e   ea 0c 00 00   2b 00 00 00   a8 05 00 00   00 00" +
                "a1 00 00 00 00 00 01 00  02 00 01 ff fe 8f aa c2" +
                "c8 b8 04 d2 78 05 ca 39";
        testWriteValue(rowsHexString, "1234-01-01 12:34:56.1234");
    }

    // insert into drc1.datetime4(date) values('2022-05-20 17:28:54.1982');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "a6 05 8a 62   1e   ea 0c 00 00   2b 00 00 00   b1 06 00 00   00 00" +
                "a1 00 00 00 00 00 01 00  02 00 01 ff fe 99 ac e9" +
                "17 36 07 be 35 a7 81 e2";
        testWriteValue(rowsHexString, "2022-05-20 17:28:54.1982");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`datetime4` (
     *   `date` datetime(6) NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='datetime4测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "64 04 8a 62   13   ea 0c 00 00   35 00 00 00   62 02 00 00   00 00" +
                "a1 00 00 00 00 00 01 00  04 64 72 63 31 00 09 64" +
                "61 74 65 74 69 6d 65 34  00 01 12 01 04 00 ac eb" +
                "be d8";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "datetime4";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "datetime", null, null, null, "4", null, null, "datetime(6)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
