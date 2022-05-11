package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.header.RowsEventPostHeader;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;

/**
 * Created by @author zhuYongMing on 2019/9/6.
 */
public class WriteRowsEvent extends AbstractRowsEvent {

    @Override
    public WriteRowsEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        return this;
    }

    public WriteRowsEvent() {
    }

    public WriteRowsEvent(long serverId, final long currentEventStartPosition, RowsEventPostHeader rowsEventPostHeader,
                          long numberOfColumns, BitSet beforePresentBitMap, BitSet afterPresentBitMap, List<Row> rows,
                          List<TableMapLogEvent.Column> columns, Long checksum, LogEventType logEventType, int flags) throws IOException {
        super(serverId, currentEventStartPosition, rowsEventPostHeader, numberOfColumns, beforePresentBitMap,
                afterPresentBitMap, rows, columns, checksum, logEventType, flags);
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    @Override
    public List<List<Object>> getAfterPresentRowsValues() {
        throw new UnsupportedOperationException("WriteRowsEvent haven't after values");
    }

    @Override
    public List<Boolean> getAfterRowsKeysPresent() {
        throw new UnsupportedOperationException("WriteRowsEvent haven't after keys");
    }
}
