package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.filter.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class AviatorRegexRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<List<Object>>> {

    private AviatorRegexFilter aviatorRegexFilter;

    public AviatorRegexRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
        aviatorRegexFilter = new AviatorRegexFilter(expression);
    }

    @Override
    protected List<List<Object>> doFilterRows(List<List<Object>> values, Map<String, Integer> indices) throws Exception {
        // TODO
        return null;
    }
}
