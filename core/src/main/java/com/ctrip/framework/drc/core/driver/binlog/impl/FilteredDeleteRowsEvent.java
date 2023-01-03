package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/6/9
 */
public class FilteredDeleteRowsEvent extends DeleteRowsEvent implements LogEventMerger {

    public FilteredDeleteRowsEvent() {
    }

    public FilteredDeleteRowsEvent(DeleteRowsEvent rowsEvent, List<TableMapLogEvent.Column> columns) throws IOException {
        super(rowsEvent, columns);
    }

    @Override
    public FilteredDeleteRowsEvent extract(List<TableMapLogEvent.Column> columns) throws IOException {
        return new FilteredDeleteRowsEvent(this, columns);
    }

    @Override
    protected List<ByteBuf> getEventByteBuf(ByteBuf headByteBuf, ByteBuf payloadBuf) {
        return mergeByteBuf(headByteBuf, payloadBuf);
    }
}
