package com.ctrip.framework.drc.core.server.common.filter.column;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.common.filter.column.ColumnFilterMode.EXCLUDE;

/**
 * Created by jixinwang on 2022/12/21
 */
public class ColumnsFilterRule {

    private String mode;

    private List<String> columns;

    public ColumnsFilterRule(ColumnsFilterConfig config) {
        this.mode = config.getMode();
        List<String> columns = config.getColumns();
        if (columns != null) {
            this.columns = columns.stream().map(String::toLowerCase).collect(Collectors.toList());
        }
    }

    public void filterColumns(AbstractRowsEvent rowsEvent, List<Integer> extractedColumnsIndex) {
        rowsEvent.extractColumns(extractedColumnsIndex);
    }

    public List<Integer> getColumnsIndex(List<String> allColumnsName) {
        List<Integer> columnsIndex;
        if (EXCLUDE == ColumnFilterMode.getColumnFilterMode(mode)) {
            columnsIndex = excludeColumns(allColumnsName);
        } else {
            columnsIndex = includeColumns(allColumnsName);
        }
        return columnsIndex;
    }

    private List<Integer> includeColumns(List<String> allColumnsName) {
        List<Integer> includeColumnsIndex = Lists.newArrayList();
        for (int i = 0; i < allColumnsName.size(); i++) {
            if (columns.contains(allColumnsName.get(i).toLowerCase())) {
                includeColumnsIndex.add(i);
            }
        }
        return includeColumnsIndex;
    }

    private List<Integer> excludeColumns(List<String> allColumnsName) {
        List<Integer> includeColumnsIndex = Lists.newArrayList();
        for (int i = 0; i < allColumnsName.size(); i++) {
            if (!columns.contains(allColumnsName.get(i).toLowerCase())) {
                includeColumnsIndex.add(i);
            }
        }
        return includeColumnsIndex;
    }
}
