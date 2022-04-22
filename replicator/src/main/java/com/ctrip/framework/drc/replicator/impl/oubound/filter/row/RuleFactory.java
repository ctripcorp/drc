package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public interface RuleFactory {

    RowsFilterRule createRowsFilterRule(RowsFilterContext context);
}
