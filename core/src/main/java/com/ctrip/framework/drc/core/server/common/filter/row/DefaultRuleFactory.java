package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;

import java.lang.reflect.Constructor;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class DefaultRuleFactory implements RuleFactory {

    @Override
    public RowsFilterRule createRowsFilterRule(RowsFilterConfig config) throws Exception {

        RowsFilterType rowFilterType = config.getRowsFilterType();
        Class<? extends RowsFilterRule> rowsFilterRule;

        if (RowsFilterType.TripUid == rowFilterType) {
            rowsFilterRule = UidRowsFilterRule.class;
        } else if (RowsFilterType.AviatorRegex == rowFilterType) {
            rowsFilterRule = AbstractRowsFilterRule.class;
        } else if (RowsFilterType.Custom == rowFilterType) {
            String clazz = System.getProperty(ROWS_FILTER_RULE);
            rowsFilterRule = (Class<RowsFilterRule>) Class.forName(clazz);
        } else {
            rowsFilterRule = NoopRowsFilterRule.class;
        }

        Constructor constructor = rowsFilterRule.getConstructor(new Class[]{RowsFilterConfig.class});
        return (RowsFilterRule) constructor.newInstance(config);
    }
}
