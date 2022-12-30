package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
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

    private long filteredEventSize;

    private boolean noRewrite = false;

    private LogEvent logEvent;

    private Map<Long, TableMapLogEvent> tableMapWithinTransaction;

    private Map<String, TableMapLogEvent> drcTableMap;

    private Map<String, Columns> extractedColumnsMap;

    private Map<String, List<Integer>> extractedColumnsIndexMap;

    private boolean skipEvent = false;

    private Exception cause;

    private String gtid;

    public OutboundLogEventContext() {
    }

    public OutboundLogEventContext(FileChannel fileChannel, long fileChannelPos, LogEventType eventType, long eventSize, String gtid) {
        this.fileChannel = fileChannel;
        this.fileChannelPos = fileChannelPos;
        this.eventType = eventType;
        this.eventSize = eventSize;
        this.gtid = gtid;
        this.filteredEventSize = eventSize;
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

    public LogEvent getLogEvent() {
        return logEvent;
    }

    public String getGtid() {
        return gtid;
    }

    public TableMapLogEvent getTableMapWithinTransaction(Long tableId) {
        if (tableMapWithinTransaction == null) {
            return null;
        }
        return tableMapWithinTransaction.get(tableId);
    }

    public TableMapLogEvent getDrcTableMap(String tableName) {
        if (drcTableMap == null) {
            return null;
        }
        return drcTableMap.get(tableName);
    }

    public Columns getExtractedColumns(String tableName) {
        if (extractedColumnsMap == null) {
            return null;
        }
        return extractedColumnsMap.get(tableName);
    }

    public List<Integer> getExtractedColumnsIndex(String tableName) {
        if (extractedColumnsIndexMap == null) {
            return null;
        }
        return extractedColumnsIndexMap.get(tableName);
    }

    public Exception getCause() {
        return cause;
    }

    public void setTableMapWithinTransaction(Map<Long, TableMapLogEvent> tableMapWithinTransaction) {
        this.tableMapWithinTransaction = tableMapWithinTransaction;
    }

    public void setDrcTableMap(Map<String, TableMapLogEvent> drcTableMap) {
        this.drcTableMap = drcTableMap;
    }

    public void setExtractedColumnsMap(Map<String, Columns> extractedColumnsMap) {
        this.extractedColumnsMap = extractedColumnsMap;
    }

    public void setExtractedColumnsIndexMap(Map<String, List<Integer>> extractedColumnsIndexMap) {
        this.extractedColumnsIndexMap = extractedColumnsIndexMap;
    }

    public void setLogEvent(LogEvent logEvent) {
        this.logEvent = logEvent;
    }

    public void setCause(Exception cause) {
        this.cause = cause;
    }

    public void backToHeader() {
        try {
            this.fileChannel.position(fileChannelPos - eventHeaderLengthVersionGt1);
        } catch (IOException e) {
            setCause(e);
        }
    }

    public void restorePosition() {
        try {
            this.fileChannel.position(fileChannelPos);
        } catch (IOException e) {
            setCause(e);
        }
    }

    public long getFilteredEventSize() {
        return filteredEventSize;
    }

    public void setFilteredEventSize(long filteredEventSize) {
        this.filteredEventSize = filteredEventSize;
    }

    public boolean isSkipEvent() {
        return skipEvent;
    }

    public void setSkipEvent(boolean skipEvent) {
        this.skipEvent = skipEvent;
    }

    public boolean isNoRewrite() {
        return noRewrite;
    }

    public void setNoRewrite(boolean noRewrite) {
        this.noRewrite = noRewrite;
    }
}
