package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public interface RowsFilterRule {

    List<Boolean> filterRow(List<AbstractRowsEvent.Row> rows);
}
