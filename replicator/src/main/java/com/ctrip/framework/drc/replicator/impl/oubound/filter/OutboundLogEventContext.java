package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.enums.LineFilterType;
import io.netty.channel.Channel;

import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundLogEventContext {

    private Channel channel;

    private FileChannel fileChannel;

    private long fileChannelPos;

    private LineFilterType filterType;

    private ConsumeType consumeType;

    private LogEventType eventType;

    private long eventSize;

    private Map<Long, TableMapLogEvent> tableNameWithinTransaction;

    private LogEvent rowsEvent;

    private boolean lineFilter = true;

    public OutboundLogEventContext(Channel channel, FileChannel fileChannel, long fileChannelPos, LineFilterType filterType, ConsumeType consumeType, LogEventType eventType, long eventSize, Map<Long, TableMapLogEvent> tableNameWithinTransaction) {
        this.channel = channel;
        this.fileChannel = fileChannel;
        this.fileChannelPos = fileChannelPos;
        this.filterType = filterType;
        this.consumeType = consumeType;
        this.eventType = eventType;
        this.eventSize = eventSize;
        this.tableNameWithinTransaction = tableNameWithinTransaction;
    }

    public Channel getChannel() {
        return channel;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public LineFilterType getFilterType() {
        return filterType;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public LogEventType getEventType() {
        return eventType;
    }

    public long getEventSize() {
        return eventSize;
    }

    public long getFileChannelPos() {
        return fileChannelPos;
    }

    public boolean isLineFilter() {
        return lineFilter;
    }

    public LogEvent getRowsEvent() {
        return rowsEvent;
    }

    // write api
    public void setLineFilter(boolean lineFilter) {
        this.lineFilter = lineFilter;
    }

    public TableMapLogEvent putTableMap(Long tableId, TableMapLogEvent tableMapLogEvent) {
        return this.tableNameWithinTransaction.put(tableId, tableMapLogEvent);
    }

    public void clearTableMap() {
        for (TableMapLogEvent tableMapLogEvent : tableNameWithinTransaction.values()) {
            tableMapLogEvent.release();
        }
        this.tableNameWithinTransaction.clear();
    }

    public void setRowsEvent(LogEvent rowsEvent) {
        this.rowsEvent = rowsEvent;
    }
}
