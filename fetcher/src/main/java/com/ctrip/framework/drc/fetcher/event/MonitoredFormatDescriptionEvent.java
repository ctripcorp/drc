package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.FormatDescriptionLogEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemoryAware;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jixinwang on 2021/9/26
 */
public class MonitoredFormatDescriptionEvent extends FormatDescriptionLogEvent implements DirectMemoryAware, FetcherEvent {

    private DirectMemory directMemory;

    private AtomicBoolean released = new AtomicBoolean(false);

    @Override
    public void release() {
        if (released.compareAndSet(false, true)) {
            directMemory.release(getLogEventHeader().getEventSize());
            super.release();
        }
    }

    @Override
    public void setDirectMemory(DirectMemory directMemory) {
        this.directMemory = directMemory;
    }
}
