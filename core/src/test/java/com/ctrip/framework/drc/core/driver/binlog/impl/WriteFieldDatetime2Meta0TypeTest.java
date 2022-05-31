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
public class WriteFieldDatetime2Meta0TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.datetime0(date) values('1000-01-01 00:00:00');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "4c ff 89 62   1e   ea 0c 00 00   29 00 00 00   89 02 00 00   00 00" +
                "9f 00 00 00 00 00 01 00  02 00 01 ff fe 8c b2 42" +
                "00 00 e1 f3 5a 16";
        testWriteValue(rowsHexString, "1000-01-01 00:00:00");
    }

    // insert into drc1.datetime0(date) values('9999-12-31 23:59:59');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "37 00 8a 62   1e   ea 0c 00 00   29 00 00 00   99 04 00 00   00 00" +
                "9f 00 00 00 00 00 01 00  02 00 01 ff fe fe f3 ff" +
                "7e fb 7f e1 4e 0a";
        testWriteValue(rowsHexString, "9999-12-31 23:59:59");
    }

    // insert into drc1.datetime0(date) values('0000-01-01 00:00:00');
    @Test
    public void testRealMin() throws IOException {
        String rowsHexString = "66 00 8a 62   1e   ea 0c 00 00   29 00 00 00   a0 05 00 00   00 00" +
                "9f 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 42" +
                "00 00 2a 19 80 76";
        testWriteValue(rowsHexString, "0000-01-01 00:00:00");
    }

    // insert into drc1.datetime0(date) values('1234-01-01 12:34:56');
    @Test
    public void testSpecificValue1() throws IOException {
        String rowsHexString = "a2 00 8a 62   1e   ea 0c 00 00   29 00 00 00   a7 06 00 00   00 00" +
                "9f 00 00 00 00 00 01 00  02 00 01 ff fe 8f aa c2" +
                "c8 b8 c8 cf 84 2c";
        testWriteValue(rowsHexString, "1234-01-01 12:34:56");
    }

    // insert into drc1.datetime0(date) values('2022-05-20 17:22:54');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "1b 01 8a 62   1e   ea 0c 00 00   29 00 00 00   ae 07 00 00   00 00" +
                "9f 00 00 00 00 00 01 00  02 00 01 ff fe 99 ac e9" +
                "15 b6 76 1c 40 01";
        testWriteValue(rowsHexString, "2022-05-20 17:22:54");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`datetime0` (
     *   `date` datetime(0) NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='datetime0测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "ec fd 89 62   13   ea 0c 00 00   35 00 00 00   59 01 00 00   00 00" +
                "9f 00 00 00 00 00 01 00  04 64 72 63 31 00 09 64" +
                "61 74 65 74 69 6d 65 30  00 01 12 01 00 00 5f 02" +
                "c4 03";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "datetime0";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "datetime", null, null, null, "0", null, null, "datetime(0)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
