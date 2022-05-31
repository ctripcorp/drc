package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// 8bytes
// [-2^63, 2^63 - 1], [-9223372036854775808, 9223372036854775807],
// unsigned [0, 2^64 - 1], [0, 18446744073709551615]
// mysql literal is bigint
public class WriteFieldLongLongTypeTest extends AbstractWriteFieldTypeTest {

    // insert into drc1.longlong(id) values(1234);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "47 e4 85 62   1e   ea 0c 00 00   2c 00 00 00   52 03 00 00   00 00" +
                "7c 00 00 00 00 00 01 00  02 00 01 ff fe d2 04 00" +
                "00 00 00 00 00 cb ba 29  0c";
        testWriteValue(rowsHexString, "1234");
    }

    // insert into drc1.longlong(id) values(-1234);
    @Test
    public void testNegative() throws IOException {
        String rowsHexString = "46 e5 85 62   1e   ea 0c 00 00   2c 00 00 00   6a 05 00 00   00 00" +
                "7c 00 00 00 00 00 01 00  02 00 01 ff fe 2e fb ff" +
                "ff ff ff ff ff 77 d2 cc  38";
        testWriteValue(rowsHexString, "-1234");
    }

    // insert into drc1.longlong(id) values(9223372036854775807);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "93 e5 85 62   1e   ea 0c 00 00   2c 00 00 00   76 06 00 00   00 00" +
                "7c 00 00 00 00 00 01 00  02 00 01 ff fe ff ff ff" +
                "ff ff ff ff 7f 4c 2f cc  c0";
        testWriteValue(rowsHexString, "9223372036854775807");
    }

    // insert into drc1.longlong(id) values(-9223372036854775808);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "e0 e5 85 62   1e   ea 0c 00 00   2c 00 00 00   82 07 00 00   00 00" +
                "7c 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 00 80 db 55 64  55";
        testWriteValue(rowsHexString, "-9223372036854775808");
    }

    // insert into drc1.longlong(id) values(0);
    @Test
    public void testZero() throws IOException {
        String rowsHexString = "1e e6 85 62   1e   ea 0c 00 00   2c 00 00 00   8e 08 00 00   00 00" +
                "7c 00 00 00 00 00 01 00  02 00 01 ff fe 00 00 00" +
                "00 00 00 00 00 77 c6 be  03";
        testWriteValue(rowsHexString, "0");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`longlong` (
     *   `id` bigint NOT NULL COMMENT 'id'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='longlong测试表';
     */
    @Override
    protected String getTableMapEventHexString() {
        return "47 e4 85 62   13   ea 0c 00 00   33 00 00 00   26 03 00 00   00 00" +
                "7c 00 00 00 00 00 01 00  04 64 72 63 31 00 08 6c" +
                "6f 6e 67 6c 6f 6e 67 00  01 08 00 00 d5 41 34 52";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "longlong";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("id", false, "bigint", null, null, null, null, null, null, "bigint", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
