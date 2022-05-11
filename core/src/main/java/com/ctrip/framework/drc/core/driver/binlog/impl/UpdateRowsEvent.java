package com.ctrip.framework.drc.core.driver.binlog.impl;


import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.RowsEventPostHeader;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.BitSet;
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

    public UpdateRowsEvent(long serverId, final long currentEventStartPosition, RowsEventPostHeader rowsEventPostHeader,
                          long numberOfColumns, BitSet beforePresentBitMap, BitSet afterPresentBitMap, List<Row> rows,
                          List<TableMapLogEvent.Column> columns, Long checksum, LogEventType logEventType, int flags) throws IOException {
        super(serverId, currentEventStartPosition, rowsEventPostHeader, numberOfColumns, beforePresentBitMap,
                afterPresentBitMap, rows, columns, checksum, logEventType, flags);
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }
}
