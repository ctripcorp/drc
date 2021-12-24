package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.QueryLogEvent;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemoryAware;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class MonitoredQueryEvent<T extends BaseTransactionContext> extends QueryLogEvent implements MetaEvent.Write<LinkContext>, DirectMemoryAware, TransactionEvent<T> {

    protected static final Logger logger = LoggerFactory.getLogger(MonitoredQueryEvent.class);

    private int dataIndex;

    private String gtid;

    private DirectMemory directMemory;

    public MonitoredQueryEvent() {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("event", "query", 1, 0);
    }

    private AtomicBoolean released = new AtomicBoolean(false);

    @Override
    public void release() {
        if (released.compareAndSet(false, true)) {
            directMemory.release(getLogEventHeader().getEventSize());
            super.release();
        }
    }

    @Override
    public void involve(LinkContext linkContext) {
        gtid = linkContext.fetchGtid();
        dataIndex = linkContext.increaseDataIndexByOne();
        linkContext.updateExecuteTime(getLogEventHeader().getEventTimestamp());
    }

    public String identifier() {
        try {
                return gtid + "-" + dataIndex + "-Q";
        } catch (Throwable t) {
            return getClass().getSimpleName() + ":UNKNOWN";
        }
    }

    @Override
    public ApplyResult apply(T context) {
        release();
        return ApplyResult.SUCCESS;
    }

    @Override
    public void setDirectMemory(DirectMemory directMemory) {
        this.directMemory = directMemory;
    }
}
