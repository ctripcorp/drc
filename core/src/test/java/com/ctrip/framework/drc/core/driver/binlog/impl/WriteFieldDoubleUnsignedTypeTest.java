package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// double 0，(2.2250738585072014E-308，1.7976931348623157E+308)
// see https://bbs.huaweicloud.com/forum/thread-90199-1-1.html
public class WriteFieldDoubleUnsignedTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.double(amount) values(12.25);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "57 43 86 62   1e   ea 0c 00 00   2c 00 00 00   95 0a 00 00   00 00" +
                "8d 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 80 28 40 72 5e 52  05";
        testWriteValue(rowsHexString, "12.25");
    }

    // insert into drc1.double(amount) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "f0 43 86 62   1e   ea 0c 00 00   2c 00 00 00   9c 0b 00 00   00 00" +
                "8d 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 00 00 8a 35 8b  86";
        testWriteValue(rowsHexString, "0.0");
    }

    // insert into drc1.double(amount) values(2.2250738585072014E-308);
    @Test
    public void testPositiveMin() throws IOException {
        String rowsHexString = "30 44 86 62   1e   ea 0c 00 00   2c 00 00 00   a3 0c 00 00   00 00" +
                "8d 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 10 00 9e 00 0c  7b";
        testWriteValue(rowsHexString, "2.2250738585072014E-308");
    }

    // insert into drc1.double(amount) values(1.7976931348623157E308);
    @Test
    public void testPositiveMax() throws IOException {
        String rowsHexString = "f1 44 86 62   1e   ea 0c 00 00   2c 00 00 00   aa 0d 00 00   00 00" +
                "8d 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff ff ff ef 7f 5e 7f 07  99";
        testWriteValue(rowsHexString, "1.7976931348623157E308");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`double` (
     *  `amount` double unsigned NOT NULL COMMENT '价格'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='double测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "57 43 86 62   13   ea 0c 00 00   32 00 00 00   69 0a 00 00   00 00" +
                "8d 00 00 00 00 00 01 00  04 64 72 63 31 00 06 64" +
                "6f 75 62 6c 65 00 01 05  01 08 00 02 b8 3c c7";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "double";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("amount", false, "double", null, null, null, null, null, null, "double unsigned", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
