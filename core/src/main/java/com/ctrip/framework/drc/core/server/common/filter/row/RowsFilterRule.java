package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public interface RowsFilterRule<V> {

    RowsFilterResult<V> filterRows(AbstractRowsEvent rowsEvent, TableMapLogEvent drcTableMapLogEvent) throws Exception;
}
