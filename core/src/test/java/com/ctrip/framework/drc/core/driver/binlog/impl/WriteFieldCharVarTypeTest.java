package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/5/19
 */
// include binary, char
// char存储字符数[0-255], 无论何种字符集; binary没有字符集
// include varbinary, varchar;
// varchar[M], M=[0-65535], 存储字符数取值要看字符集
public class WriteFieldCharVarTypeTest extends AbstractWriteFieldTypeTest {

    // insert into `drc1`.`char` (`china`, `japan`, `southkorea`) values('中国1', '愚かな1', '안녕하세요');
    @Test
    public void testValue() throws IOException {
        String rowsHexString = "02 77 86 62   1e   ea 0c 00 00   49 00 00 00   a7 01 00 00   00 00" +
                "8f 00 00 00 00 00 01 00  02 00 03 ff f8 11 d0 bf" +
                "d1 80 d0 b8 d1 81 d0 be  d1 81 d0 ba d0 b0 31 04" +
                "00 61 62 63 31 0b 00 68  65 6c 6c 6f 20 77 6f 72" +
                "6c 64 03 37 e3 d4";
        testWriteValue(rowsHexString, "присоска1");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`varchar` (
     *    `varcharlt256` varchar(30) CHARACTER SET utf8mb4,
     *    `varchareq256` varchar(256) CHARACTER SET latin1,
     *    `varchargt256` varchar(2000) CHARACTER SET utf8
     * ) ENGINE=InnoDB;
     */
    @Override
    protected String getTableMapEventHexString() {
        return "02 77 86 62   13   ea 0c 00 00   3a 00 00 00   5e 01 00 00   00 00" +
                "8f 00 00 00 00 00 01 00  04 64 72 63 31 00 07 76" +
                "61 72 63 68 61 72 00 03  0f 0f 0f 06 78 00 00 01" +
                "70 17 07 7f 10 b3 20";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "varchar";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("varcharlt256", true, "varchar", "30", null, null, null, "utf8mb4", "utf8mb4_general_ci", "varchar(30)", null, null, null),
                new TableMapLogEvent.Column("varchareq256", true, "varchar", "256", null, null, null, "latin1", "latin1_swedish_ci", "varchar(128)", null, null, null),
                new TableMapLogEvent.Column("varchargt256", true, "varchar", "2000", null, null, null, "utf8", "utf8_general_ci", "varchar(255)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
