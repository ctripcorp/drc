package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.core.AllTests.ROW_FILTER_PROPERTIES;
import static com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory.ROWS_FILTER_RULE;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class DefaultRuleFactoryTest {

    private static final String registryKey = "ut_key";

    private static final String location = "CN";

    private RuleFactory ruleFactory = new DefaultRuleFactory();

    @Test
    public void createRowsFilterRule() throws Exception {
        String properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.TripUdl.getName(), location, location);
        DataMediaConfig dataMediaConfig = DataMediaConfig.from(registryKey, properties);
        RowsFilterRule rowsFilterRule = ruleFactory.createRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
        Assert.assertTrue(rowsFilterRule instanceof UidRowsFilterRule);

        properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.None.getName(), location, location);
        dataMediaConfig = DataMediaConfig.from(registryKey, properties);
        rowsFilterRule = ruleFactory.createRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
        Assert.assertTrue(rowsFilterRule instanceof NoopRowsFilterRule);

        properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.AviatorRegex.getName(), location, location);
        dataMediaConfig = DataMediaConfig.from(registryKey, properties);
        rowsFilterRule = ruleFactory.createRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
        Assert.assertTrue(rowsFilterRule instanceof AviatorRegexRowsFilterRule);

        properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.JavaRegex.getName(), location, location);
        dataMediaConfig = DataMediaConfig.from(registryKey, properties);
        rowsFilterRule = ruleFactory.createRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
        Assert.assertTrue(rowsFilterRule instanceof JavaRegexRowsFilterRule);

        System.setProperty(ROWS_FILTER_RULE, "com.ctrip.framework.drc.core.server.common.filter.row.CustomRowsFilterRule");
        properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.Custom.getName(), location, location);
        dataMediaConfig = DataMediaConfig.from(registryKey, properties);
        rowsFilterRule = ruleFactory.createRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
        Assert.assertTrue(rowsFilterRule instanceof CustomRowsFilterRule);
    }
}