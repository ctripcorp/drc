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
        String rowsHexString = "81 89 84 62   1e   ea 0c 00 00   27 00 00 00   96 02 00 00   00 00" +
                "75 00 00 00 00 00 01 00  02 00 01 ff fe 80 c3 4f" +
                "5a dc 5d 4c";
        testWriteValue(rowsHexString, "12:13:15");
    }

    // insert into drc1.time2(start_at) values('-12:13:15');
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "32 8b 84 62   1e   ea 0c 00 00   27 00 00 00   97 03 00 00   00 00" +
                "75 00 00 00 00 00 01 00  02 00 01 ff fe 7f 3c b1" +
                "b3 55 8e b3";
        testWriteValue(rowsHexString, "-12:13:15");
    }

    // insert into drc1.time2(start_at) values('838:59:59');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "63 8b 84 62   1e   ea 0c 00 00   27 00 00 00   98 04 00 00   00 00" +
                "75 00 00 00 00 00 01 00  02 00 01 ff fe b4 6e fb" +
                "bc 61 71 e1";
        testWriteValue(rowsHexString, "838:59:59");
    }

    // insert into drc1.time2(start_at) values('-838:59:59.00');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "88 8b 84 62   1e   ea 0c 00 00   27 00 00 00   99 05 00 00   00 00" +
                "75 00 00 00 00 00 01 00  02 00 01 ff fe 4b 91 05" +
                "1a b8 81 36";
        testWriteValue(rowsHexString, "-838:59:59");
    }

    // insert into drc1.time2(start_at) values('00:00:00');
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "df 8b 84 62   1e   ea 0c 00 00   27 00 00 00   9a 06 00 00   00 00" +
                "75 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "51 9f 06 59";
        testWriteValue(rowsHexString, "00:00:00");
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
