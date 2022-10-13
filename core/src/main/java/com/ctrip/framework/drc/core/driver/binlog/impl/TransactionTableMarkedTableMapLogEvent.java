package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * @Author limingdong
 * @create 2022/10/12
 */
public class TransactionTableMarkedTableMapLogEvent implements LogEvent {

    private TableMapLogEvent delegate;

    public TransactionTableMarkedTableMapLogEvent() {
        delegate = new TableMapLogEvent();
    }

    public TransactionTableMarkedTableMapLogEvent(TableMapLogEvent logEvent) {
        this.delegate = logEvent;
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
    public TransactionTableMarkedTableMapLogEvent read(ByteBuf byteBuf) {
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

    public long getTableId() {
        return delegate.getTableId();
    }

    public int getFlags() {
        return delegate.getFlags();
    }

    public String getSchemaName() {
        return delegate.getSchemaName();
    }

    public String getTableName() {
        return delegate.getTableName();
    }

    public String getSchemaNameDotTableName() {
        return delegate.getSchemaNameDotTableName();
    }

    public long getColumnsCount() {
        return delegate.getColumnsCount();
    }

    public List<TableMapLogEvent.Column> getColumns() {
        return delegate.getColumns();
    }

    public Long getChecksum() {
        return delegate.getChecksum();
    }

    public List<List<String>> getIdentifiers() {
        return delegate.getIdentifiers();
    }

    public TableMapLogEvent getDelegate() {
        return delegate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionTableMarkedTableMapLogEvent)) return false;
        TransactionTableMarkedTableMapLogEvent that = (TransactionTableMarkedTableMapLogEvent) o;
        return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {

        return Objects.hash(delegate);
    }

}
