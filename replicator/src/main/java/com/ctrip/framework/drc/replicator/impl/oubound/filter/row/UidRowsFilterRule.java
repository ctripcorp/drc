package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.filter.row.AbstractRowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class UidRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<List<Object>>> {

    public UidRowsFilterRule(String context) {
        super(context);
    }

    @Override
    protected List<List<Object>> doRowsFilter(List<List<Object>> values, List<Integer> indices) {
        return null;
    }
}
