package com.ctrip.framework.drc.core.server.common.filter.column;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by jixinwang on 2022/12/21
 */
public class ColumnsFilterRule {

    private String mode;

    private List<String> columns;

    public ColumnsFilterRule(ColumnsFilterConfig config) {
        this.mode = config.getMode();
        this.columns = config.getColumns();
    }

    public void filterColumns(AbstractRowsEvent rowsEvent, List<String> allColumnsName) {
        List<Integer> columnsIndex = getColumnsIndex(allColumnsName);
        rowsEvent.extractColumns(columnsIndex);
    }

    public List<Integer> getColumnsIndex(List<String> allColumnsName) {
        List<Integer> columnsIndex;
        if(mode.equalsIgnoreCase("exclude")) {
            columnsIndex = excludeColumns(allColumnsName);
        } else {
            columnsIndex = includeColumns(allColumnsName);
        }
        return columnsIndex;
    }

    private List<Integer> includeColumns(List<String> allColumnsName) {
        List<Integer> ret = Lists.newArrayList();
        for (int i = 0; i < allColumnsName.size(); i++) {
            if (columns.contains(allColumnsName.get(i))) {
                ret.add(i);
            }
        }
        return ret;
    }

    private List<Integer> excludeColumns(List<String> allColumnsName) {
        List<Integer> ret = Lists.newArrayList();
        for (int i = 0; i < allColumnsName.size(); i++) {
            if (!columns.contains(allColumnsName.get(i))) {
                ret.add(i);
            }
        }
        return ret;
    }
}
