package com.ctrip.framework.drc.core.server.manager;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;

import java.util.List;
import java.util.Optional;

import static com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult.Status.No_Filter_Rule;

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
    public RowsFilterResult filterRows(AbstractRowsEvent rowsEvent, RowsFilterContext rowsFilterContext) throws Exception {
        String tableName = rowsFilterContext.getDrcTableMapLogEvent().getSchemaNameDotTableName();
        Optional<RowsFilterRule> optional = dataMediaConfig.getRowsFilterRule(tableName);
        if (optional.isEmpty()) {
            return new RowsFilterResult(No_Filter_Rule);
        }
        RowsFilterRule rowsFilterRule = optional.get();
        return rowsFilterRule.filterRows(rowsEvent, rowsFilterContext);
    }
}
