package com.ctrip.framework.drc.core.driver.binlog.impl;

import java.io.IOException;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/6/9
 */
public class FilteredWriteRowsEvent extends WriteRowsEvent {

    public FilteredWriteRowsEvent() {
    }

    public FilteredWriteRowsEvent(WriteRowsEvent rowsEvent, List<TableMapLogEvent.Column> columns) throws IOException {
        super(rowsEvent, columns);
    }

    @Override
    public FilteredWriteRowsEvent extract(List<TableMapLogEvent.Column> columns) throws IOException {
        return new FilteredWriteRowsEvent(this, columns);
    }
}
