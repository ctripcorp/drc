package com.ctrip.framework.drc.replicator.impl.oubound.filter.sender;

import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.replicator.impl.oubound.ReplicatorException;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;


/**
 * @author yongnian
 */
public class SenderOutboundLogEventContext extends OutboundLogEventContext {

    public void refresh(OutboundLogEventContext origin) {
        this.reset(origin.getFileChannelPos(), origin.getFileChannelSize());
        this.setLogEvent(origin.getLogEvent());
        this.setEventType(origin.getEventType());
        this.setEventSize(origin.getEventSize());
        this.setFileChannel(origin.getFileChannel());
        this.setCompositeByteBuf(origin.getCompositeByteBuf());
    }

    @Override
    public void skipPosition(Long skipSize) {
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
