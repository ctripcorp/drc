package com.ctrip.framework.drc.core.server.common.filter.row;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public interface RuleFactory {

    String ROWS_FILTER_RULE = "drc.rows.filter.rule";

    RowsFilterRule createRowsFilterRule(RowsFilterContext context) throws Exception;
}
