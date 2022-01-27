package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.BeginEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemoryAware;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.ctrip.framework.drc.fetcher.resource.context.EventGroupContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author limingdong
 * @create 2021/3/16
 */
public class MonitoredGtidLogEvent<T extends BaseTransactionContext> extends GtidLogEvent implements MetaEvent.Write<EventGroupContext>, DirectMemoryAware, BeginEvent<T> {

    protected static final Logger loggerEGS = LoggerFactory.getLogger("EVENT GROUP SEQUENCE");

    private String gtid;

    private String identifier;

    private DirectMemory directMemory;

    public MonitoredGtidLogEvent() {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("event", "gtid", 1, 0);
    }

    public MonitoredGtidLogEvent(String gtid) {
        super(gtid);
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
        context.begin(getGtid());
    }

    public String identifier() {
        try {
            if (StringUtils.isBlank(identifier)) {
                StringBuilder stringBuilder = new StringBuilder(256);
                stringBuilder.append(getGtid()).append("(").append(getLastCommitted()).append(", ").append(getSequenceNumber()).append(")");
                identifier = stringBuilder.toString();
            }
            return identifier;
        } catch (Throwable t) {
            return getClass().getSimpleName() + ":UNKNOWN";
        }
    }

    public String getGtid() {
        if (StringUtils.isBlank(gtid)) {
            gtid = super.getGtid();
        }
        return gtid;
    }

    @Override
    public TransactionData.ApplyResult apply(T context) {
        release();
        loggerEGS.info("set gtid next for {}", getGtid());
        context.updateGtid(getGtid());
        context.updateSequenceNumber(getSequenceNumber());
        context.resetTableKeyMap();
        return TransactionData.ApplyResult.SUCCESS;
    }

    @Override
    public void setDirectMemory(DirectMemory directMemory) {
        this.directMemory = directMemory;
    }
}
