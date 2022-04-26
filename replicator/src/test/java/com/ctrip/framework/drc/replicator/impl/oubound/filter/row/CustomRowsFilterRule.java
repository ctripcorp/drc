package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.filter.row.AbstractRowsFilterRule;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class CustomRowsFilterRule extends AbstractRowsFilterRule {

    public CustomRowsFilterRule(String context) {
        super(context);
    }

    @Override
    protected List<List<Object>> doFilterRows(List<List<Object>> values, List<Integer> indices) {
        return null;
    }
}
