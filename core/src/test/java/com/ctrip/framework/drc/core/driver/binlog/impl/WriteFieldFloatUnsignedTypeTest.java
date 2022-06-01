package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/18
 */
// float unsigned 0，(1.175494351E-38，3.402823466E+38)
// see https://bbs.huaweicloud.com/forum/thread-90199-1-1.html
public class WriteFieldFloatUnsignedTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.float(amount) values(12.34);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "b9 c9 84 62   1e   ea 0c 00 00   28 00 00 00   a1 28 00 00   00 00" +
                "79 00 00 00 00 00 01 00  02 00 01 ff fe a4 70 45" +
                "41 95 7a eb c9";
        testWriteValue(rowsHexString, "12.34");
    }

    // insert into drc1.float(amount) values(3.402823466E+38);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "1e c9 84 62   1e   ea 0c 00 00   28 00 00 00   9d 26 00 00   00 00" +
                "79 00 00 00 00 00 01 00  02 00 01 ff fe ff ff 7f" +
                "7f cc 93 0a 31";
        testWriteValue(rowsHexString, "3.4028235E38");
    }

    // insert into drc1.float(amount) values(1.175494351E-38);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "58 c8 84 62   1e   ea 0c 00 00   28 00 00 00   9b 25 00 00   00 00" +
                "79 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 80" +
                "00 f3 8a 02 e3";
        testWriteValue(rowsHexString, "1.17549435E-38");
    }

    // insert into drc1.float(amount) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "6c c9 84 62   1e   ea 0c 00 00   28 00 00 00   9f 27 00 00   00 00" +
                "79 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 51 95 f1 f9";
        testWriteValue(rowsHexString, "0.0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`float` (
     *  `amount` float unsigned NOT NULL COMMENT '价格'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='float测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "58 c8 84 62   13   ea 0c 00 00   31 00 00 00   73 25 00 00   00 00" +
                "79 00 00 00 00 00 01 00  04 64 72 63 31 00 05 66" +
                "6c 6f 61 74 00 01 04 01  04 00 2c 3a 15 f2";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "float";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("amount", false, "float", null, null, null, null, null, null, "float unsigned", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
