package com.ctrip.framework.drc.core.driver.binlog.impl;


import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public class UpdateRowsEvent extends AbstractRowsEvent {

    @Override
    public UpdateRowsEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        return this;
    }

    public UpdateRowsEvent() {
    }

    public UpdateRowsEvent(UpdateRowsEvent rowsEvent, List<TableMapLogEvent.Column> columns) throws IOException {
        super(rowsEvent, columns);
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }
}
