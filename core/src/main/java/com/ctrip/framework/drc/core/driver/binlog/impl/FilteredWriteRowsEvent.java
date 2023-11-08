package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/6/9
 */
public class FilteredWriteRowsEvent extends WriteRowsEvent implements LogEventMerger {

    public FilteredWriteRowsEvent() {
    }

    public FilteredWriteRowsEvent(WriteRowsEvent rowsEvent, List<TableMapLogEvent.Column> columns) throws IOException {
        super(rowsEvent, columns);
    }

    @Override
    public FilteredWriteRowsEvent extract(List<TableMapLogEvent.Column> columns) throws IOException {
        return new FilteredWriteRowsEvent(this, columns);
    }

    @Override
    protected List<ByteBuf> getEventByteBuf(ByteBuf headByteBuf, ByteBuf payloadBuf) {
        return mergeByteBuf(headByteBuf, payloadBuf);
    }
}
