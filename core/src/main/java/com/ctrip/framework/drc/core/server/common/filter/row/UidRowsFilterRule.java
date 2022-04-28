package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class UidRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<List<Object>>> {

    public UidRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
    }

    @Override
    protected List<List<Object>> doFilterRows(List<List<Object>> values, Map<String, Integer> indices) throws Exception {
        // TODO
        return null;
    }
}
