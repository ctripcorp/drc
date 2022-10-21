package com.ctrip.framework.drc.core.server.common.enums;

import com.ctrip.framework.drc.core.server.common.filter.row.*;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.core.server.common.enums.RowsFilterType.JavaRegex;
import static com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory.ROWS_FILTER_RULE;

/**
 * @Author limingdong
 * @create 2022/9/21
 */
public class RowsFilterTypeTest {

    @Test
    public void filterRuleClass() throws ClassNotFoundException {
        System.setProperty(ROWS_FILTER_RULE, "com.ctrip.framework.drc.core.server.common.filter.row.CustomRowsFilterRule");
        Class<? extends RowsFilterRule> clazz = RowsFilterType.Custom.filterRuleClass();
        Assert.assertTrue(clazz == CustomRowsFilterRule.class);

        clazz = RowsFilterType.None.filterRuleClass();
        Assert.assertTrue(clazz == NoopRowsFilterRule.class);

        clazz = RowsFilterType.AviatorRegex.filterRuleClass();
        Assert.assertTrue(clazz == AviatorRegexRowsFilterRule.class);

        clazz = JavaRegex.filterRuleClass();
        Assert.assertTrue(clazz == JavaRegexRowsFilterRule.class);

        clazz = RowsFilterType.TripUdl.filterRuleClass();
        Assert.assertTrue(clazz == UserRowsFilterRule.class);

        clazz = RowsFilterType.TripUid.filterRuleClass();
        Assert.assertTrue(clazz == UserRowsFilterRule.class);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testName() {
        RowsFilterType filterType = RowsFilterType.getType(JavaRegex.getName().toUpperCase());
        Assert.assertTrue(JavaRegex == filterType);

        RowsFilterType.getType("exception");
    }
}