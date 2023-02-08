package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.column.ColumnsFilterRule;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public interface RuleFactory {

    String ROWS_FILTER_RULE = "drc.rows.filter.rule";

    RowsFilterRule createRowsFilterRule(RowsFilterConfig config) throws Exception;

    ColumnsFilterRule createColumnsFilterRule(ColumnsFilterConfig config) throws Exception;
}
