package com.ctrip.framework.drc.core.server.common.filter.row;

/**
 * @Author limingdong
 * @create 2022/4/27
 */
public class RowsFilterRuleWrapper {

    private boolean match;

    private RowsFilterRule rowsFilterRule;

    public RowsFilterRuleWrapper(boolean match, RowsFilterRule rowsFilterRule) {
        this.match = match;
        this.rowsFilterRule = rowsFilterRule;
    }

    public boolean isMatch() {
        return match;
    }

    public RowsFilterRule getRowsFilterRule() {
        return rowsFilterRule;
    }
}
