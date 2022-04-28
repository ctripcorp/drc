package com.ctrip.framework.drc.core.server.manager;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRuleWrapper;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/27
 */
public class DataMediaManager implements RowsFilterRule<List<List<Object>>> {

    private DataMediaConfig dataMediaConfig;

    public DataMediaManager(DataMediaConfig dataMediaConfig) {
        this.dataMediaConfig = dataMediaConfig;
    }

    @Override
    public RowsFilterResult filterRows(AbstractRowsEvent rowsEvent, TableMapLogEvent drcTableMapLogEvent) throws Exception {
        String tableName = drcTableMapLogEvent.getSchemaNameDotTableName();
        RowsFilterRuleWrapper wrapper = dataMediaConfig.getRowsFilterRule(tableName);
        if (wrapper == null || !wrapper.isMatch()) {
            return new RowsFilterResult(true);
        }
        RowsFilterRule rowsFilterRule = wrapper.getRowsFilterRule();
        return rowsFilterRule.filterRows(rowsEvent, drcTableMapLogEvent);
    }
}
