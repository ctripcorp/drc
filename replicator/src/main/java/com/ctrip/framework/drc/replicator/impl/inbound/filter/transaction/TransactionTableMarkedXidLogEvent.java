package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.impl.XidLogEvent;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Objects;

/**
 * @Author limingdong
 * @create 2022/9/30
 */
public class TransactionTableMarkedXidLogEvent implements LogEvent {

    private XidLogEvent delegate;

    public TransactionTableMarkedXidLogEvent(XidLogEvent delegate) {
        this.delegate = delegate;
    }

    @Override
    public LogEventType getLogEventType() {
        return delegate.getLogEventType();
    }

    @Override
    public LogEventHeader getLogEventHeader() {
        return delegate.getLogEventHeader();
    }

    @Override
    public ByteBuf getPayloadBuf() {
        return delegate.getPayloadBuf();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public TransactionTableMarkedXidLogEvent read(ByteBuf byteBuf) {
        delegate.read(byteBuf);
        return this;
    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {
        delegate.write(byteBuf);
    }

    @Override
    public void write(IoCache ioCache) {
        delegate.write(ioCache);
    }

    @Override
    public void release() throws Exception {
        delegate.release();
    }

    public long getXid() {
        return delegate.getXid();
    }

    public Long getChecksum() {
        return delegate.getChecksum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionTableMarkedXidLogEvent)) return false;
        TransactionTableMarkedXidLogEvent that = (TransactionTableMarkedXidLogEvent) o;
        return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {

        return Objects.hash(delegate);
    }
}
