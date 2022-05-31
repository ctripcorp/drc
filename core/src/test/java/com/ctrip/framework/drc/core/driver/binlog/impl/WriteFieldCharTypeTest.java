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
public class WriteFieldCharTypeTest extends AbstractWriteFieldTypeTest {

    // insert into `drc1`.`char` (`china`, `japan`, `southkorea`) values('中国1', '愚かな1', '안녕하세요');
    @Test
    public void testValue() throws IOException {
        String rowsHexString = "fc 67 86 62   1e   ea 0c 00 00   3f 00 00 00   fd 0f 00 00   00 00" +
                "8e 00 00 00 00 00 01 00  02 00 03 ff f8 05 d6 d0" +
                "b9 fa 31 07 00 8b f0 82  a9 82 c8 31 0a 00 be c8" +
                "b3 e7 c7 cf bc bc bf e4  6b e6 31 8a";
        testWriteValue(rowsHexString, "中国1");
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`char` (
     *    `china` char(30) CHARACTER SET gbk,
     *    `japan` char(128) CHARACTER SET cp932,
     *    `southkorea` char(255) CHARACTER SET euckr
     * ) ENGINE=InnoDB;
     */
    @Override
    protected String getTableMapEventHexString() {
        return "fc 67 86 62   13   ea 0c 00 00   37 00 00 00   be 0f 00 00   00 00" +
                "8e 00 00 00 00 00 01 00  04 64 72 63 31 00 04 63" +
                "68 61 72 00 03 fe fe fe  06 fe 3c ee 00 ee fe 07" +
                "fd c7 2e 83";
    }

    @Override
    protected TableMapLogEvent getDrcTableMapEvent() throws IOException {
        String schemaName = "drc1";
        String tableName = "char";
        ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
                new TableMapLogEvent.Column("china", true, "char", "90", null, null, null, "gbk", "gbk_chinese_ci", "char(30)", null, null, null),
                new TableMapLogEvent.Column("japan", true, "char", "384", null, null, null, "cp932", "cp932_japanese_ci", "char(128)", null, null, null),
                new TableMapLogEvent.Column("southkorea", true, "char", "765", null, null, null, "euckr", "euckr_korean_ci", "char(255)", null, null, null)
        );
        List<List<String>> identifiers = Lists.<List<String>>newArrayList(
                Lists.<String>newArrayList()
        );
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }
}
