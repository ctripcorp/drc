package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory;

import java.lang.reflect.Constructor;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class DefaultRuleFactory implements RuleFactory {

    @Override
    public RowsFilterRule createRowsFilterRule(RowsFilterContext context) throws Exception {

        RowFilterType rowFilterType = context.getFilterType();
        Class<? extends RowsFilterRule> rowsFilterRule;

        if (RowFilterType.Uid == rowFilterType) {
            rowsFilterRule = UidRowsFilterRule.class;
        } else if (RowFilterType.Custom == rowFilterType) {
            String clazz = System.getProperty(ROWS_FILTER_RULE);
            rowsFilterRule = (Class<RowsFilterRule>) Class.forName(clazz);
        } else {
            rowsFilterRule = NoopRowsFilterRule.class;
        }

        Constructor constructor = rowsFilterRule.getConstructor(new Class[]{RowsFilterContext.class});
        return (RowsFilterRule) constructor.newInstance(context);
    }
}
