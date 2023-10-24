package com.ctrip.framework.drc.core.driver.binlog.impl;

import java.io.IOException;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/6/9
 */
public class FilteredUpdateRowsEvent extends UpdateRowsEvent {

    public FilteredUpdateRowsEvent() {
    }

    public FilteredUpdateRowsEvent(UpdateRowsEvent rowsEvent, List<TableMapLogEvent.Column> columns) throws IOException {
        super(rowsEvent, columns);
    }

    @Override
    public FilteredUpdateRowsEvent extract(List<TableMapLogEvent.Column> columns) throws IOException {
        return new FilteredUpdateRowsEvent(this, columns);
    }
}
