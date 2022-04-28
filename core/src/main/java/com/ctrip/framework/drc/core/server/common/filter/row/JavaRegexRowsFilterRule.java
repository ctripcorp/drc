package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class JavaRegexRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<List<Object>>> {

    private Pattern pattern;

    public JavaRegexRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
        pattern = Pattern.compile(expression);
    }

    @Override
    protected List<List<Object>> doFilterRows(List<List<Object>> values, Map<String, Integer> indices) throws Exception {
        // TODO
        return null;
    }
}
