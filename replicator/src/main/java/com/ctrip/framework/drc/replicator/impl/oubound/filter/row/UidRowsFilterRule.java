package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class UidRowsFilterRule implements RowsFilterRule {

    private Map<String, String> table2Uid;

    public UidRowsFilterRule(Map<String, String> table2Uid) {
        this.table2Uid = table2Uid;
    }

    @Override
    public boolean filterRow(AbstractRowsEvent.Row row) {
        return false;
    }
}
