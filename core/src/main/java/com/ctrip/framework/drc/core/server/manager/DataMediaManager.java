package com.ctrip.framework.drc.core.server.manager;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.common.filter.column.ColumnsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

import static com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult.Status.No_Filtered;

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
            return new RowsFilterResult(No_Filtered);
        }
        RowsFilterRule rowsFilterRule = optional.get();
        return rowsFilterRule.filterRows(rowsEvent, rowsFilterContext);
    }

    public boolean filterColumns(AbstractRowsEvent rowsEvent, String tableName, List<Integer> extractedColumnsIndex) {
        Optional<ColumnsFilterRule> optional = dataMediaConfig.getColumnsFilterRule(tableName);
        if (optional.isEmpty()) {
            return false;
        }
        ColumnsFilterRule columnsFilterRule = optional.get();
        columnsFilterRule.filterColumns(rowsEvent, extractedColumnsIndex);
        return true;
    }

    public List<Integer> getExtractedColumnsIndex(TableMapLogEvent tableMapLogEvent) {
        Optional<ColumnsFilterRule> optional = dataMediaConfig.getColumnsFilterRule(tableMapLogEvent.getSchemaNameDotTableName());
        if (optional.isEmpty()) {
            return null;
        }
        ColumnsFilterRule columnsFilterRule = optional.get();
        return columnsFilterRule.getColumnsIndex(tableMapLogEvent.getColumnsName());
    }

    public boolean hasColumnsFilter(String tableName) {
        Optional<ColumnsFilterRule> optional = dataMediaConfig.getColumnsFilterRule(tableName);
        return optional.isPresent();
    }
}
