package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;
import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-07
 */
public class ConfigServiceImplTest {

    private ConfigServiceImpl configService;

    @Before
    public void setUp() {
        configService = new ConfigServiceImpl();
    }

    @Test
    public void testGetAllDrcSupportDateTypes() {
        final String[] allDrcSupportDataTypes = configService.getAllDrcSupportDataTypes();
        List<String> dataTypeList = Lists.newArrayList(allDrcSupportDataTypes);
        EnumSet.allOf(MysqlFieldType.class).forEach(mysqlFieldType -> {
            final String[] literals = mysqlFieldType.getLiterals();
            for(String literal : literals) {
                Assert.assertTrue(dataTypeList.contains(literal));
            }
        });

        Assert.assertNotEquals(allDrcSupportDataTypes.length, 0);
    }
}
