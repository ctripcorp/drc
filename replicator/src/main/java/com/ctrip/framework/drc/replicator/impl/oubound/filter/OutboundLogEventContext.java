package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;

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

    private Map<Long, TableMapLogEvent> tableMapWithinTransaction;

    private Map<String, TableMapLogEvent> drcTableMap;

    private boolean skip = false;

    private IOException cause;

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

    public boolean isSkip() {
        return skip;
    }

    public LogEvent getRowsEvent() {
        return rowsEvent;
    }

    public TableMapLogEvent getTableMapWithinTransaction(Long tableId) {
        return tableMapWithinTransaction.get(tableId);
    }

    public TableMapLogEvent getDrcTableMap(String tableName) {
        return drcTableMap.get(tableName);
    }

    public IOException getCause() {
        return cause;
    }

    // write api
    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    public void setTableMapWithinTransaction(Map<Long, TableMapLogEvent> tableMapWithinTransaction) {
        this.tableMapWithinTransaction = tableMapWithinTransaction;
    }

    public void setDrcTableMap(Map<String, TableMapLogEvent> drcTableMap) {
        this.drcTableMap = drcTableMap;
    }

    public void setRowsEvent(LogEvent rowsEvent) {
        this.rowsEvent = rowsEvent;
    }

    public void backToHeader() {
        try {
            this.fileChannel.position(fileChannelPos - eventHeaderLengthVersionGt1);
        } catch (IOException e) {
            this.cause = e;
        }
    }
}
