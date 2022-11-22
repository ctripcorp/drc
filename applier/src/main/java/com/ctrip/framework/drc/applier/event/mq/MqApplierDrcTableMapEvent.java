package com.ctrip.framework.drc.applier.event.mq;

import com.ctrip.framework.drc.applier.event.ApplierDrcTableMapEvent;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;

/**
 * Created by jixinwang on 2022/10/25
 */
public class MqApplierDrcTableMapEvent extends ApplierDrcTableMapEvent {

    protected void logEvent() {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.event", "drc table map", 1, 0);
    }
}
