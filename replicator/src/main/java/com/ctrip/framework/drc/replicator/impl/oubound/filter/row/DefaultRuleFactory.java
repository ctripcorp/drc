package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class DefaultRuleFactory implements RuleFactory {

    @Override
    public RowsFilterRule createRowsFilterRule(RowsFilterContext context) {
        if (RowFilterType.Uid == context.getFilterType()) {
            return new UidRowsFilterRule((Map<String, String>) context.getFilterContext());
        }
        return new NoopRowsFilterRule();
    }
}
