package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;

import java.nio.channels.FileChannel;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundLogEventContext {

    private FileChannel fileChannel;

    private long fileChannelPos;

    private LogEventType eventType;

    private long eventSize;

    private LogEvent rowsEvent;

    private boolean lineFilter = true;

    public OutboundLogEventContext(FileChannel fileChannel, long fileChannelPos, LogEventType eventType, long eventSize) {
        this.fileChannel = fileChannel;
        this.fileChannelPos = fileChannelPos;
        this.eventType = eventType;
        this.eventSize = eventSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
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

    public void setRowsEvent(LogEvent rowsEvent) {
        this.rowsEvent = rowsEvent;
    }
}
