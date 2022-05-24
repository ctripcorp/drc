package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class NoopRowsFilterRule implements RowsFilterRule<Void> {

    public NoopRowsFilterRule(RowsFilterConfig rowsFilterConfig) {

    }

    @Override
    public RowsFilterResult<Void> filterRows(AbstractRowsEvent rowsEvent, RowsFilterContext rowsFilterContext) {
        return new RowsFilterResult(false);
    }
}
