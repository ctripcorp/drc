package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;
import com.google.common.collect.Maps;
import io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundLogEventContext {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private FileChannel fileChannel;

    private long fileSeq;

    /**
     * cached initial fileChannel position
     */
    private long fileChannelPos;

    /**
     * cached initial fileChannel size
     */
    private long fileChannelSize;

    protected LogEventType eventType;

    private long eventSize;

    private boolean rewrite;

    private CompositeByteBuf compositeByteBuf;

    protected LogEvent logEvent;

    private Map<Long, TableMapLogEvent> rowsRelatedTableMap = Maps.newHashMap();

    private boolean skipEvent = false;

    private Exception cause;

    private String gtid;

    private boolean everSeeGtid;

    private boolean inExcludeGroup = false;

    public OutboundLogEventContext() {
    }

    public OutboundLogEventContext(FileChannel fileChannel, long fileChannelPos, long fileChannelSize) {
        this.fileChannel = fileChannel;
        this.fileChannelPos = fileChannelPos;
        this.fileChannelSize = fileChannelSize;
    }

    public BinlogPosition getBinlogPosition() {
        return BinlogPosition.from(fileSeq, fileChannelPos);
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }
    public void setFileSeq(long fileSeq) {
        this.fileSeq = fileSeq;
    }
    public void setFileChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    public void setFileChannelPos(long fileChannelPos) {
        this.fileChannelPos = fileChannelPos;
    }

    public long getFileChannelSize() {
        return fileChannelSize;
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

    public Map<Long, TableMapLogEvent> getRowsRelatedTableMap() {
        return rowsRelatedTableMap;
    }

    public Exception getCause() {
        return cause;
    }

    public void setCause(Exception cause) {
        this.cause = cause;
    }

    public boolean isSkipEvent() {
        return skipEvent;
    }

    public void setSkipEvent(boolean skipEvent) {
        this.skipEvent = skipEvent;
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

    public boolean isInExcludeGroup() {
        return inExcludeGroup;
    }

    public void setInExcludeGroup(boolean inExcludeGroup) {
        this.inExcludeGroup = inExcludeGroup;
    }

    public void reset(long fileChannelPos, long fileChannelSize) {
        this.cause = null;
        this.fileChannelPos = fileChannelPos;
        this.fileChannelSize = fileChannelSize;
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
            AbstractRowsEvent rowsEvent = newRowsEvent(eventType);
            EventReader.readEvent(fileChannel, eventSize, rowsEvent, compositeByteBuf);
            logEvent = rowsEvent;
        }

        return (AbstractRowsEvent) logEvent;
    }

    protected AbstractRowsEvent newRowsEvent(LogEventType eventType) {
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
        return rowsEvent;
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

    /**
     * To save extra fileChannel.position() call
     *
     * @see OutboundLogEventContext#skipPosition(Long)
     */
    public void skipPositionAfterReadEvent(Long skipSize) {
        try {
            // After read event body, fileChannelPos + eventSize == fileChannel.position()
            fileChannel.position(fileChannelPos + eventSize + skipSize);
        } catch (IOException e) {
            logger.error("skip position error:", e);
            setCause(e);
            setSkipEvent(true);
        }
    }
}
