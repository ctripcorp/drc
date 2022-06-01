package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/21
 */
public class WriteFieldNewdecimalOffset0TypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.newdecimal0(amount) values(123.46);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "ca df 89 62   1e   ea 0c 00 00   2d 00 00 00   b2 02 00 00   00 00" +
                "9b 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 00 00 7b 2e 05 69  16 67";
        testWriteValue(rowsHexString, "123.46");
    }

    // insert into drc1.newdecimal0(amount) values(-123.46);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "e2 e6 89 62   1e   ea 0c 00 00   2d 00 00 00   c0 03 00 00   00 00" +
                "9b 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff ff ff ff 84 d1 b7 4c  b2 cc";
        testWriteValue(rowsHexString, "-123.46");
    }

    // insert into drc1.newdecimal0(amount) values(999999999999999999.99);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "79 e8 89 62   1e   ea 0c 00 00   2d 00 00 00   ce 04 00 00   00 00" +
                "9b 00 00 00 00 00 01 00  02 00 01 ff fe bb 9a c9" +
                "ff 3b 9a c9 ff 63 ab 1b  18 78";
        testWriteValue(rowsHexString, "999999999999999999.99");
    }

    // insert into drc1.newdecimal0(amount) values(-999999999999999999.99);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "2f ea 89 62   1e   ea 0c 00 00   2d 00 00 00   dc 05 00 00   00 00" +
                "9b 00 00 00 00 00 01 00  02 00 01 ff fe 44 65 36" +
                "00 c4 65 36 00 9c cd b2  d7 71";
        testWriteValue(rowsHexString, "-999999999999999999.99");
    }

    // insert into drc1.newdecimal0(amount) values(999999999.99);
    @Test
    public void testInt9() throws IOException {
        String rowsHexString = "ac ea 89 62   1e   ea 0c 00 00   2d 00 00 00   ea 06 00 00   00 00" +
                "9b 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 3b 9a c9 ff 63 0d 15  00 11";
        testWriteValue(rowsHexString, "999999999.99");
    }

    // insert into drc1.newdecimal0(amount) values(0.1);
    @Test
    public void testScale1() throws IOException {
        String rowsHexString = "f9 ea 89 62   1e   ea 0c 00 00   2d 00 00 00   f8 07 00 00   00 00" +
                "9b 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 00 00 00 0a 2e 24  6f 4e";
        testWriteValue(rowsHexString, "0.10");
    }

    // insert into drc1.newdecimal0(amount) values(-0.1);
    @Test
    public void testNegativeScale1() throws IOException {
        String rowsHexString = "4f eb 89 62   1e   ea 0c 00 00   2d 00 00 00   06 09 00 00   00 00" +
                "9b 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff ff ff ff ff f5 22 87  40 48";
        testWriteValue(rowsHexString, "-0.10");
    }

    // insert into drc1.newdecimal0(amount) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "fb f3 89 62   1e   ea 0c 00 00   2d 00 00 00   d7 14 00 00   00 00" +
                "9b 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 00 00 00 00 f8 c4  cd 56";
        testWriteValue(rowsHexString, "0.00");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`newdecimal0` (
     *   `amount` decimal(20,2) NOT NULL COMMENT '金额'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='newdecimal0测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "ca df 89 62   13   ea 0c 00 00   38 00 00 00   85 02 00 00   00 00" +
                "9b 00 00 00 00 00 01 00  04 64 72 63 31 00 0b 6e" +
                "65 77 64 65 63 69 6d 61  6c 30 00 01 f6 02 14 02" +
                "00 63 ef 9f 45";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "newdecimal0";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("amount", false, "decimal", null, "20", "2", null, null, null, "decimal(20,2)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
