package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.utils.MySqlUtils;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MysqlServiceV2ImplTest {

    @Test
    public void getExistDrcMonitorTables() {
        List<String> tables = new ArrayList<>();
        tables.add("dly_db1");
        tables.add("tx_db1");
        tables.add("dly_db2");
        tables.add("tx_db3");
        Set<String> existDrcMonitorTables = MySqlUtils.getDbHasDrcMonitorTables(tables);
        Assert.assertTrue(existDrcMonitorTables.contains("db1"));
        Assert.assertFalse(existDrcMonitorTables.contains("db2"));
        Assert.assertFalse(existDrcMonitorTables.contains("db3"));
        Assert.assertFalse(existDrcMonitorTables.contains("db4"));
        Assert.assertEquals(1, existDrcMonitorTables.size());


        Assert.assertEquals(0, MySqlUtils.getDbHasDrcMonitorTables(Lists.newArrayList()).size());
    }
}