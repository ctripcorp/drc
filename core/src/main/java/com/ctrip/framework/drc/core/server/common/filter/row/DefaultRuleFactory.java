package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.column.ColumnsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.column.DefaultColumnsFilterRule;

import java.lang.reflect.Constructor;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class DefaultRuleFactory implements RuleFactory {

    @Override
    public RowsFilterRule createRowsFilterRule(RowsFilterConfig config) throws Exception {

        RowsFilterType rowFilterType = config.getRowsFilterType();
        Class<? extends RowsFilterRule> rowsFilterRule = rowFilterType.filterRuleClass();
        Constructor constructor = rowsFilterRule.getConstructor(new Class[]{RowsFilterConfig.class});
        return (RowsFilterRule) constructor.newInstance(config);
    }

    @Override
    public ColumnsFilterRule createColumnsFilterRule(ColumnsFilterConfig config) throws Exception {
        return new DefaultColumnsFilterRule(config);
    }

}
