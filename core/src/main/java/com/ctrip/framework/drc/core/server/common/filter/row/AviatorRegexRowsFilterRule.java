package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.filter.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class AviatorRegexRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<AbstractRowsEvent.Row>> {

    private AviatorRegexFilter aviatorRegexFilter;

    public AviatorRegexRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
        aviatorRegexFilter = new AviatorRegexFilter(context);
    }

    @Override
    protected List<AbstractRowsEvent.Row> doFilterRows(AbstractRowsEvent rowsEvent, LinkedHashMap<String, Integer> indices) throws Exception {
        // TODO
        return null;
    }
}
