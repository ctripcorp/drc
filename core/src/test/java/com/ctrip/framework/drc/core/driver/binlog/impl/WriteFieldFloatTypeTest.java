package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/18
 */
// float (-3.402823466E+38，-1.175494351E-38)，0，(1.175494351E-38，3.402823466351E+38)
// see https://bbs.huaweicloud.com/forum/thread-90199-1-1.html
public class WriteFieldFloatTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.float(amount) values(12.34);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "14 c4 84 62   1e   ea 0c 00 00   28 00 00 00   b1 1d 00 00   00 00" +
                "78 00 00 00 00 00 01 00  02 00 01 ff fe a4 70 45" +
                "41 86 09 23 0b";
        testWriteValue(rowsHexString, "12.34");
    }

    // insert into drc1.float(amount) values(-12.34);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "74 c4 84 62   1e   ea 0c 00 00   28 00 00 00   b3 1e 00 00   00 00" +
                "78 00 00 00 00 00 01 00  02 00 01 ff fe a4 70 45" +
                "c1 04 b3 61 18";
        testWriteValue(rowsHexString, "-12.34");
    }

    // insert into drc1.float(amount) values(-3.402823466E+38);
    @Test
    public void testNegativeMin() throws IOException {
        String rowsHexString = "51 be 84 62   1e   ea 0c 00 00   28 00 00 00   a7 18 00 00   00 00" +
                "78 00 00 00 00 00 01 00  02 00 01 ff fe ff ff 7f" +
                "ff 08 c4 5e 77";
        testWriteValue(rowsHexString, "-3.4028235E38");
    }

    // insert into drc1.float(amount) values(-1.175494351E-38);
    @Test
    public void testNegativeMax() throws IOException {
        String rowsHexString = "cf c5 84 62   1e   ea 0c 00 00   28 00 00 00   b7 20 00 00   00 00" +
                "78 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 80" +
                "80 c3 74 ea 97";
        testWriteValue(rowsHexString, "-1.17549435E-38");
    }

    // insert into drc1.float(amount) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "df c4 84 62   1e   ea 0c 00 00   28 00 00 00   b5 1f 00 00   00 00" +
                "78 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 17 09 6a 7f";
        testWriteValue(rowsHexString, "0.0");
    }

    // insert into drc1.float(amount) values(1.175494351E-38);
    @Test
    public void testPositiveMin() throws IOException {
        String rowsHexString = "a1 c6 84 62   1e   ea 0c 00 00   28 00 00 00   b9 21 00 00   00 00" +
                "78 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 80" +
                "00 bf 9d ae 98";
        testWriteValue(rowsHexString, "1.17549435E-38");
    }

    // insert into drc1.float(amount) values(3.402823466351E+38);
    @Test
    public void testPositiveMax() throws IOException {
        String rowsHexString = "05 c7 84 62   1e   ea 0c 00 00   28 00 00 00   bb 22 00 00   00 00" +
                "78 00 00 00 00 00 01 00  02 00 01 ff fe ff ff 7f" +
                "7f 97 c6 b1 f6";
        testWriteValue(rowsHexString, "3.4028235E38");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`float` (
     *  `amount` float NOT NULL COMMENT '价格'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='float测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "c2 c0 84 62   13   ea 0c 00 00   31 00 00 00   87 1c 00 00   00 00" +
                "78 00 00 00 00 00 01 00  04 64 72 63 31 00 05 66" +
                "6c 6f 61 74 00 01 04 01  04 00 3e bb be 6a";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "float";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("amount", false, "float", null, null, null, null, null, null, "float", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
