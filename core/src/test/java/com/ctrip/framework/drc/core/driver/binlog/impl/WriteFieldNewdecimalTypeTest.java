package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/16
 */
public class WriteFieldNewdecimalTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.newdecimal(amount) values(123.456);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "b4 29 83 62   1e   ea 0c 00 00   30 00 00 00   eb 11 00 00   00 00" +
                "6e 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 7b 1b 2e 02 00 00  00 5c b9 d8 f5";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(-123.456);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "61 2a 83 62   1e   ea 0c 00 00   30 00 00 00   fb 12 00 00   00 00" +
                "6e 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff ff 84 e4 d1 fd ff ff  ff e6 21 76 5f";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(9999999999999.999999999999);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "c3 2a 83 62   1e   ea 0c 00 00   30 00 00 00   0b 14 00 00   00 00" +
                "6e 00 00 00 00 00 01 00  02 00 01 ff fe a7 0f 3b" +
                "9a c9 ff 3b 9a c9 ff 03  e7 29 aa a6 f0";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(-9999999999999.999999999999);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "0c 2b 83 62   1e   ea 0c 00 00   30 00 00 00   1b 15 00 00   00 00" +
                "6e 00 00 00 00 00 01 00  02 00 01 ff fe 58 f0 c4" +
                "65 36 00 c4 65 36 00 fc  18 03 20 9e bd";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(999999999.999);
    @Test
    public void testInt9() throws IOException {
        String rowsHexString = "5b 2b 83 62   1e   ea 0c 00 00   30 00 00 00   2b 16 00 00   00 00" +
                "6e 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 3b" +
                "9a c9 ff 3b 8b 87 c0 00  00 74 66 87 a0";
        testWriteValue(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(0.1);
    @Test
    public void testScale1() throws IOException {
        String rowsHexString = "8d 2b 83 62   1e   ea 0c 00 00   30 00 00 00   3b 17 00 00   00 00" +
                "6e 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 00 05 f5 e1 00 00  00 3f f3 ef ca";
        testWriteValue(rowsHexString);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`newdecimal` (
     *   `amount` decimal(25,12) NOT NULL COMMENT '金额'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='newdecimal测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "b4 29 83 62   13   ea 0c 00 00   37 00 00 00   bb 11 00 00   00 00" +
                "6e 00 00 00 00 00 01 00  04 64 72 63 31 00 0a 6e" +
                "65 77 64 65 63 69 6d 61  6c 00 01 f6 02 19 0c 00" +
                "ad 11 62 48";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "newdecimal";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("amount", false, "decimal", null, "25", "12", null, null, null, "decimal(25,12)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
