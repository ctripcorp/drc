package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class NoopRowsFilterRule implements RowsFilterRule {

    @Override
    public List<Boolean> filterRow(List<AbstractRowsEvent.Row> rows) {
        return Lists.newArrayList(false);
    }
}
