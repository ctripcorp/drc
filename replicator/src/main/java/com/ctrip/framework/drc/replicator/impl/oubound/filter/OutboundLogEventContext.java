package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.google.common.collect.Maps;
import io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private FileChannel fileChannel;

    private long fileChannelPos;

    private LogEventType eventType;

    private long eventSize;

    private boolean noRewrite = false;

    private boolean rewrite;

    private CompositeByteBuf compositeByteBuf;

    private LogEvent logEvent;

    private Map<Long, TableMapLogEvent> tableMapWithinTransaction;

    private Map<Long, TableMapLogEvent> rowsRelatedTableMap = Maps.newHashMap();

    private Map<String, TableMapLogEvent> drcTableMap;

    private Map<String, List<Integer>> extractedColumnsIndexMap;

    private boolean skipEvent = false;

    private Exception cause;

    private String gtid;

    private boolean everSeeGtid;

    public OutboundLogEventContext() {
    }

    public OutboundLogEventContext(FileChannel fileChannel, long fileChannelPos) {
        this.fileChannel = fileChannel;
        this.fileChannelPos = fileChannelPos;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public void setFileChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    public void setFileChannelPos(long fileChannelPos) {
        this.fileChannelPos = fileChannelPos;
    }

    public LogEventType getEventType() {
        return eventType;
    }

    public void setEventType(LogEventType eventType) {
        this.eventType = eventType;
    }

    public long getEventSize() {
        return eventSize;
    }

    public void setEventSize(long eventSize) {
        this.eventSize = eventSize;
    }

    public long getFileChannelPos() {
        return fileChannelPos;
    }

    public boolean isRewrite() {
        return rewrite;
    }

    public void setRewrite(boolean rewrite) {
        this.rewrite = rewrite;
    }

    public CompositeByteBuf getCompositeByteBuf() {
        return compositeByteBuf;
    }

    public void setCompositeByteBuf(CompositeByteBuf compositeByteBuf) {
        this.compositeByteBuf = compositeByteBuf;
    }

    public void setLogEvent(LogEvent logEvent) {
        this.logEvent = logEvent;
    }

    public LogEvent getLogEvent() {
        return logEvent;
    }

    public TableMapLogEvent getTableMapWithinTransaction(Long tableId) {
        if (tableMapWithinTransaction == null) {
            return null;
        }
        return tableMapWithinTransaction.get(tableId);
    }

    public Map<Long, TableMapLogEvent> getRowsRelatedTableMap() {
        return rowsRelatedTableMap;
    }

    public void setRowsRelatedTableMap(Map<Long, TableMapLogEvent> rowsRelatedTableMap) {
        this.rowsRelatedTableMap = rowsRelatedTableMap;
    }

    public Map<String, TableMapLogEvent> getDrcTableMap() {
        return drcTableMap;
    }

    public void setDrcTableMap(Map<String, TableMapLogEvent> drcTableMap) {
        this.drcTableMap = drcTableMap;
    }

    public TableMapLogEvent getDrcTableMap(String tableName) {
        if (drcTableMap == null) {
            return null;
        }
        return drcTableMap.get(tableName);
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

    public void setExtractedColumnsIndexMap(Map<String, List<Integer>> extractedColumnsIndexMap) {
        this.extractedColumnsIndexMap = extractedColumnsIndexMap;
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

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public String getGtid() {
        return gtid;
    }

    public boolean isEverSeeGtid() {
        return everSeeGtid;
    }

    public void setEverSeeGtid(boolean everSeeGtid) {
        this.everSeeGtid = everSeeGtid;
    }

    public void reset(long fileChannelPos) {
        this.cause = null;
        this.fileChannelPos = fileChannelPos;
        this.skipEvent = false;
        this.rewrite = false;
        this.logEvent = null;
    }

    public DrcIndexLogEvent readIndexLogEvent() {
        if (logEvent == null) {
            DrcIndexLogEvent drcIndexLogEvent = new DrcIndexLogEvent();
            EventReader.readEvent(fileChannel, eventSize, drcIndexLogEvent, compositeByteBuf);
            logEvent = drcIndexLogEvent;
        }

        return (DrcIndexLogEvent) logEvent;
    }

    public GtidLogEvent readGtidEvent() {
        if (logEvent == null) {
            GtidLogEvent gtidLogEvent = new GtidLogEvent();
            EventReader.readEvent(fileChannel, eventSize, gtidLogEvent, compositeByteBuf);
            logEvent = gtidLogEvent;
        }

        return (GtidLogEvent) logEvent;
    }

    public FilterLogEvent readFilterEvent() {
        if (logEvent == null) {
            FilterLogEvent filterLogEvent = new FilterLogEvent();
            EventReader.readEvent(fileChannel, eventSize, filterLogEvent, compositeByteBuf);
            logEvent = filterLogEvent;
        }

        return (FilterLogEvent) logEvent;
    }

    public TableMapLogEvent readTableMapEvent() {
        if (logEvent == null) {
            TableMapLogEvent tableMapLogEvent = new TableMapLogEvent();
            EventReader.readEvent(fileChannel, eventSize, tableMapLogEvent, compositeByteBuf);
            logEvent = tableMapLogEvent;
        }

        return (TableMapLogEvent) logEvent;
    }

    public AbstractRowsEvent readRowsEvent() {
        if (logEvent == null) {
            AbstractRowsEvent rowsEvent;
            switch (eventType) {
                case write_rows_event_v2:
                    rowsEvent = new FilteredWriteRowsEvent();
                    break;
                case update_rows_event_v2:
                    rowsEvent = new FilteredUpdateRowsEvent();
                    break;
                case delete_rows_event_v2:
                    rowsEvent = new FilteredDeleteRowsEvent();
                    break;
                default:
                    throw new RuntimeException("row event type does not exist: " + eventType);
            }

            EventReader.readEvent(fileChannel, eventSize, rowsEvent, compositeByteBuf);
            logEvent = rowsEvent;
        }

        return (AbstractRowsEvent) logEvent;
    }

    public void skipPosition(Long skipSize) {
        try {
            fileChannel.position(fileChannel.position() + skipSize);
        } catch (IOException e) {
            logger.error("skip position error:", e);
            setCause(e);
            setSkipEvent(true);
        }
    }
}
