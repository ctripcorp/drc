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

        switch (rowFilterType) {
            case TripUid:
                rowsFilterRule = UidRowsFilterRule.class;
                break;
            case AviatorRegex:
                rowsFilterRule = AviatorRegexRowsFilterRule.class;
                break;
            case JavaRegex:
                rowsFilterRule = JavaRegexRowsFilterRule.class;
                break;
            case Custom:
                String clazz = System.getProperty(ROWS_FILTER_RULE);
                rowsFilterRule = (Class<RowsFilterRule>) Class.forName(clazz);
                break;
            default:
                rowsFilterRule = NoopRowsFilterRule.class;
        }

        Constructor constructor = rowsFilterRule.getConstructor(new Class[]{RowsFilterConfig.class});
        return (RowsFilterRule) constructor.newInstance(config);
    }
}
