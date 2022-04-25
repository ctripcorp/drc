package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class NoopRowsFilterRule implements RowsFilterRule<Void> {

    @Override
    public RowsFilterResult<Void> filterRow(AbstractRowsEvent rowsEvent, TableMapLogEvent tableMapLogEvent, TableMapLogEvent drcTableMapLogEvent) {
        return new RowsFilterResult(false);
    }
}
