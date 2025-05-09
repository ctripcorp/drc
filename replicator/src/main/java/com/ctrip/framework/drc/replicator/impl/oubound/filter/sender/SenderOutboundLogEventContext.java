package com.ctrip.framework.drc.replicator.impl.oubound.filter.sender;

import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.replicator.impl.oubound.ReplicatorException;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;


/**
 * @author yongnian
 */
public class SenderOutboundLogEventContext extends OutboundLogEventContext {
    private final BinlogPosition binlogPosition;

    public SenderOutboundLogEventContext() {
        this.binlogPosition = BinlogPosition.empty();
    }

    public boolean refresh(OutboundLogEventContext scannerContext) {
        if (!updatePosition(scannerContext.getBinlogPosition())) {
            return false;
        }
        this.reset(scannerContext.getFileChannelPos(), scannerContext.getFileChannelSize());
        this.setLogEvent(scannerContext.getLogEvent());
        this.setEventType(scannerContext.getEventType());
        this.setEventSize(scannerContext.getEventSize());
        this.setFileChannel(scannerContext.getFileChannel());
        this.setCompositeByteBuf(scannerContext.getCompositeByteBuf());
        return true;
    }

    public boolean updatePosition(BinlogPosition scannerBinlogPosition) {
        return this.binlogPosition.tryMoveForward(scannerBinlogPosition);
    }

    @Override
    public BinlogPosition getBinlogPosition() {
        return binlogPosition;
    }

    @Override
    public void skipEvent() {
        throw new ReplicatorException(ResultCode.UNKNOWN_ERROR, this);
    }

    @Override
    public void skipPositionAfterReadEvent(Long skipSize) {
        throw new ReplicatorException(ResultCode.UNKNOWN_ERROR, this);
    }

    private <T> T getReadEvent(Class<T> eventType) {
        if (logEvent == null || !eventType.isAssignableFrom(logEvent.getClass())) {
            throw new ReplicatorException(ResultCode.UNKNOWN_ERROR, this);
        }
        return (T) logEvent;
    }

    @Override
    public DrcIndexLogEvent readIndexLogEvent() {
        return getReadEvent(DrcIndexLogEvent.class);
    }

    @Override
    public GtidLogEvent readGtidEvent() {
        return getReadEvent(GtidLogEvent.class);
    }

    @Override
    public FilterLogEvent readFilterEvent() {
        return getReadEvent(FilterLogEvent.class);
    }

    @Override
    public TableMapLogEvent readTableMapEvent() {
        return getReadEvent(TableMapLogEvent.class);
    }

    @Override
    public AbstractRowsEvent readRowsEvent() {
        AbstractRowsEvent readEvent = getReadEvent(AbstractRowsEvent.class);
        AbstractRowsEvent rowsEvent = newRowsEvent(eventType);
        readEvent.copyTo(rowsEvent);
        return rowsEvent;
    }
}
