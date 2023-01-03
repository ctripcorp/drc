package com.ctrip.framework.drc.core.server.common.filter.column;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

import java.util.List;

/**
 * Created by jixinwang on 2022/12/30
 */
public interface ColumnsFilterRule {

    boolean filterColumns(AbstractRowsEvent rowsEvent, ColumnsFilterContext columnsFilterContext);

    List<Integer> getExtractColumnsIndex(TableMapLogEvent tableMapLogEvent);
}
