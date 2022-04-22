package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public interface RowsFilterRule {

    boolean filterRow(AbstractRowsEvent.Row row);
}
