package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/17
 */
// document show range '-838:59:59.000000' to '838:59:59.000000'
public class WriteFieldTime2TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.time2(start_at) values('12:13:14.567');
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "34 4e 83 62   1e   ea 0c 00 00   29 00 00 00   1a 1b 00 00   00 00" +
                "6f 00 00 00 00 00 01 00  02 00 01 ff fe 80 c3 4e" +
                "16 26 53 a5 b6 fd";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.time2(start_at) values('-12:13:14.567');
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "8e 51 83 62   1e   ea 0c 00 00   29 00 00 00   1d 1c 00 00   00 00" +
                "6f 00 00 00 00 00 01 00  02 00 01 ff fe 7f 3c b1" +
                "e9 da e8 c5 e3 fd";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.time2(start_at) values('838:59:59.0000');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "e4 b8 83 62   1e   ea 0c 00 00   29 00 00 00   20 1d 00 00   00 00" +
                "6f 00 00 00 00 00 01 00  02 00 01 ff fe b4 6e fb" +
                "00 00 bd 6d b1 8f";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.time2(start_at) values('-838:59:59.0000');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "8f b9 83 62   1e   ea 0c 00 00   29 00 00 00   23 1e 00 00   00 00" +
                "6f 00 00 00 00 00 01 00  02 00 01 ff fe 4b 91 05" +
                "00 00 4b e9 3e 8e";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.time2(start_at) values('-00:00:00.0100');
    @Test
    public void testNegativeMillSecond() throws IOException {
        String rowsHexString = "8f b9 83 62   1e   ea 0c 00 00   29 00 00 00   23 1e 00 00   00 00" +
                "6f 00 00 00 00 00 01 00  02 00 01 ff fe 4b 91 05" +
                "00 00 4b e9 3e 8e";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`time2` (
     *   `start_at` TIME(4) NOT NULL COMMENT '时间'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='time2测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "34 4e 83 62   13   ea 0c 00 00   31 00 00 00   f1 1a 00 00   00 00" +
                "6f 00 00 00 00 00 01 00  04 64 72 63 31 00 05 74" +
                "69 6d 65 32 00 01 13 01  04 00 60 2a ba 04";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "time2";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("start_at", false, "time", null, null, null, "4", null, null, "time(4)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
