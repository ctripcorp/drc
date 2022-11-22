package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.XidLogEvent;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TerminateEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemoryAware;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.ctrip.framework.drc.fetcher.resource.context.EventGroupContext;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class MonitoredXidEvent<T extends BaseTransactionContext> extends XidLogEvent implements MetaEvent.Write<EventGroupContext>, DirectMemoryAware, TerminateEvent<T> {

    protected final long createdTime;

    private String gtid;

    private DirectMemory directMemory;

    public MonitoredXidEvent() {
        createdTime = System.currentTimeMillis();
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
    public void involve(EventGroupContext context) {
//        gtid = context.fetchGtid();
//        context.commit();
    }

    public void updateDumpPosition(EventGroupContext context) {
        gtid = context.fetchGtid();
        context.commit();
    }

    public String identifier() {
        try {
            return gtid + "-xid";
        } catch (Throwable t) {
            return getClass().getSimpleName() + ":UNKNOWN";
        }
    }

    @Override
    public void setDirectMemory(DirectMemory directMemory) {
        this.directMemory = directMemory;
    }

    @Override
    public ApplyResult apply(T context) {
        release();
        return TransactionData.ApplyResult.SUCCESS;
    }
}
