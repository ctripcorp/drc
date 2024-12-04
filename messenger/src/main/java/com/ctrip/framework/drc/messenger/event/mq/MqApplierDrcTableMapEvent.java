package com.ctrip.framework.drc.messenger.event.mq;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.event.ApplierDrcTableMapEvent;

/**
 * Created by jixinwang on 2022/10/25
 */
public class MqApplierDrcTableMapEvent extends ApplierDrcTableMapEvent {

    protected void logEvent() {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.event", "drc table map", 1, 0);
    }
}
