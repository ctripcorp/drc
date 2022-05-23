package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/18
 */
// document show range : '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
// real range : '0000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
public class WriteFieldDatetime2Meta6TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.datetime(date) values('1000-01-01 00:00:00.000000');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "68 d7 84 62   1e   ea 0c 00 00   2c 00 00 00   56 03 00 00   00 00" +
                "7b 00 00 00 00 00 01 00  02 00 01 ff fe 8c b2 42" +
                "00 00 00 00 00 e9 04 2f  fd";
        testWriteValue(rowsHexString, "1000-01-01 00:00:00.000000");
    }

    // insert into drc1.datetime(date) values('9999-12-31 23:59:59.999999');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "1b d9 84 62   1e   ea 0c 00 00   2c 00 00 00   5f 04 00 00   00 00" +
                "7b 00 00 00 00 00 01 00  02 00 01 ff fe fe f3 ff" +
                "7e fb 0f 42 3f 8d 0b 49  a7";
        testWriteValue(rowsHexString, "9999-12-31 23:59:59.999999");
    }

    // insert into drc1.datetime(date) values('0000-01-01 00:00:00.000000');
    @Test
    public void testRealMin() throws IOException {
        String rowsHexString = "55 16 85 62   1e   ea 0c 00 00   2c 00 00 00   68 05 00 00   00 00" +
                "7b 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 42" +
                "00 00 00 00 00 ea 82 03  36";
        testWriteValue(rowsHexString, "0000-01-01 00:00:00.000000");
    }

    // insert into drc1.datetime(date) values('1000-01-01 00:00:00');
    @Test
    public void testSpecificValue1() throws IOException {
        String rowsHexString = "c3 17 85 62   1e   ea 0c 00 00   2c 00 00 00   71 06 00 00   00 00" +
                "7b 00 00 00 00 00 01 00  02 00 01 ff fe 8c b2 42" +
                "00 00 00 00 00 f9 2d 80  f3";
        testWriteValue(rowsHexString, "1000-01-01 00:00:00.000000");
    }

    // insert into drc1.datetime(date) values('2022-05-18 22:28:54.198');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "5a 1a 85 62   1e   ea 0c 00 00   2c 00 00 00   7a 07 00 00   00 00" +
                "7b 00 00 00 00 00 01 00  02 00 01 ff fe 99 ac e5" +
                "67 36 03 05 70 d8 dc 65  ee";
        testWriteValue(rowsHexString, "2022-05-18 22:28:54.198000");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`datetime` (
     *   `date` datetime(6) NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='datetime测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "68 d7 84 62   13   ea 0c 00 00   34 00 00 00   2a 03 00 00   00 00" +
                "7b 00 00 00 00 00 01 00  04 64 72 63 31 00 08 64" +
                "61 74 65 74 69 6d 65 00  01 12 01 06 00 6d 51 13" +
                "1d";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "datetime";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "datetime", null, null, null, "6", null, null, "datetime(6)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
