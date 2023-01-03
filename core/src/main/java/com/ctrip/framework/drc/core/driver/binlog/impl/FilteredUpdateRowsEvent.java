package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/6/9
 */
public class FilteredUpdateRowsEvent extends UpdateRowsEvent implements LogEventMerger {

    public FilteredUpdateRowsEvent() {
    }

    public FilteredUpdateRowsEvent(UpdateRowsEvent rowsEvent, List<TableMapLogEvent.Column> columns) throws IOException {
        super(rowsEvent, columns);
    }

    @Override
    public FilteredUpdateRowsEvent extract(List<TableMapLogEvent.Column> columns) throws IOException {
        return new FilteredUpdateRowsEvent(this, columns);
    }

    @Override
    protected List<ByteBuf> getEventByteBuf(ByteBuf headByteBuf, ByteBuf payloadBuf) {
        return mergeByteBuf(headByteBuf, payloadBuf);
    }
}
