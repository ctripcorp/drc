package com.ctrip.framework.drc.console.pojo;

import com.ctrip.xpipe.codec.JsonCodec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Author limingdong
 * @create 2021/7/8
 */
public class TableConfigsTest {

    private static final String TABLES = "{\n" +
            "    \"defaultUcsShardColumn\":\"uid\",\n" +
            "    \"tableConfigs\":[\n" +
            "        {\n" +
            "            \"tableName\":\"a\",\n" +
            "            \"ucsShardColumn\":\"azzz\",\n" +
            "            \"ignoreReplication\":false\n" +
            "        },\n" +
            "        {\n" +
            "            \"tableName\":\"b\",\n" +
            "            \"ucsShardColumn\":null,\n" +
            "            \"ignoreReplication\":true\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    private TableConfigs tableConfigs;

    @Before
    public void setUp() throws Exception {
        tableConfigs = JsonCodec.INSTANCE.decode(TABLES, TableConfigs.class);
    }

    @Test
    public void testDecode() {
        Assert.assertEquals(tableConfigs.getDefaultUcsShardColumn(), "uid");
        List<TableConfig> tableConfigList = tableConfigs.getTableConfigs();

        Assert.assertEquals(tableConfigList.size(), 2);

        for(TableConfig tableConfig : tableConfigList) {
            if ("a".equalsIgnoreCase(tableConfig.getTableName())) {
                Assert.assertEquals(tableConfig.getUcsShardColumn(), "azzz");
                Assert.assertEquals(tableConfig.isIgnoreReplication(), false);
            } else if ("b".equalsIgnoreCase(tableConfig.getTableName())) {
                Assert.assertEquals(tableConfig.getUcsShardColumn(), null);
                Assert.assertEquals(tableConfig.isIgnoreReplication(), true);
            } else {
                Assert.fail("table name not match");

            }
        }
    }
}