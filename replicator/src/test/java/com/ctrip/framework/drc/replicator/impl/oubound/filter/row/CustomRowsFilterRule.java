package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.filter.row.AbstractRowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;

import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class CustomRowsFilterRule extends AbstractRowsFilterRule {

    public CustomRowsFilterRule(RowsFilterContext rowsFilterContext) {
        super(rowsFilterContext);
    }

    @Override
    protected List<List<Object>> doFilterRows(List<List<Object>> values, Map<String, Integer> indices) {
        return null;
    }
}
