package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.AllTests.ROW_FILTER_PROPERTIES;
import static com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory.ROWS_FILTER_RULE;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class DefaultRuleFactoryTest {

    private static final String registryKey = "ut_key";

    private RuleFactory ruleFactory = new DefaultRuleFactory();

    private Map<String, List<String>> table2Id = Maps.newHashMap();

    private List<String> fields = Lists.newArrayList();

    @Test
    public void createRowsFilterRule() throws Exception {
        fields.add("id");
        table2Id.put("drc1.insert1", fields);
        String properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.TripUid.getName());
        DataMediaConfig dataMediaConfig = DataMediaConfig.from(registryKey, properties);
        RowsFilterRule rowsFilterRule = ruleFactory.createRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
        Assert.assertTrue(rowsFilterRule instanceof UidRowsFilterRule);

        properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.None.getName());
        dataMediaConfig = DataMediaConfig.from(registryKey, properties);
        rowsFilterRule = ruleFactory.createRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
        Assert.assertTrue(rowsFilterRule instanceof NoopRowsFilterRule);

        properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.AviatorRegex.getName());
        dataMediaConfig = DataMediaConfig.from(registryKey, properties);
        rowsFilterRule = ruleFactory.createRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
        Assert.assertTrue(rowsFilterRule instanceof AbstractRowsFilterRule);

        System.setProperty(ROWS_FILTER_RULE, "com.ctrip.framework.drc.core.server.common.filter.row.CustomRowsFilterRule");
        properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.Custom.getName());
        dataMediaConfig = DataMediaConfig.from(registryKey, properties);
        rowsFilterRule = ruleFactory.createRowsFilterRule(dataMediaConfig.getRowsFilters().get(0));
        Assert.assertTrue(rowsFilterRule instanceof CustomRowsFilterRule);
    }
}