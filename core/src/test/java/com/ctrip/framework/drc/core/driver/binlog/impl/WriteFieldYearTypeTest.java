package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/22
 */
// '1901' to '2155'
public class WriteFieldYearTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.year(date) values('1901');
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "b2 11 8a 62   1e   ea 0c 00 00   25 00 00 00   83 04 00 00   00 00" +
                "a6 00 00 00 00 00 01 00  02 00 01 ff fe 01 e8 54" +
                "e1 a5";
        testWriteValue(rowsHexString, "1901");
    }

    // insert into drc1.year(date) values('2155');
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "3b 12 8a 62   1e   ea 0c 00 00   25 00 00 00   80 05 00 00   00 00" +
                "a6 00 00 00 00 00 01 00  02 00 01 ff fe ff 21 5d" +
                "4a 6c";
        testWriteValue(rowsHexString, "2155");
    }

    // insert into drc1.year(date) values('2022');
    @Test
    public void testNow() throws IOException {
        String rowsHexString = "69 12 8a 62   1e   ea 0c 00 00   25 00 00 00   7d 06 00 00   00 00" +
                "a6 00 00 00 00 00 01 00  02 00 01 ff fe 7a 80 91" +
                "4c 25";
        testWriteValue(rowsHexString, "2022");
    }

    // insert into drc1.year(date) values('0000');
    // see https://dev.mysql.com/doc/refman/5.7/en/year.html
    @Test
    public void tesZero() throws IOException {
        String rowsHexString = "f6 12 8a 62   1e   ea 0c 00 00   25 00 00 00   7a 07 00 00   00 00" +
                "a6 00 00 00 00 00 01 00  02 00 01 ff fe 00 0e 5d" +
                "24 ee";
//        testWriteValue(rowsHexString, "1901");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`year` (
     *   `date` year NOT NULL COMMENT '日期'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='year';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "b2 11 8a 62   13   ea 0c 00 00   2f 00 00 00   5e 04 00 00   00 00" +
                "a6 00 00 00 00 00 01 00  04 64 72 63 31 00 04 79" +
                "65 61 72 00 01 0d 00 00  fe fb a8 68";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "year";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("date", false, "year", null, null, null, null, null, null, "year", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
