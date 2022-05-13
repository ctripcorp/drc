package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class JavaRegexRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<AbstractRowsEvent.Row>> {

    private Pattern pattern;

    public JavaRegexRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
        pattern = Pattern.compile(context);
    }

    protected List<AbstractRowsEvent.Row> doFilterRows(AbstractRowsEvent rowsEvent, LinkedHashMap<String, Integer> indices) throws Exception {
        // TODO
        return null;
    }
}
