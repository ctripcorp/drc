package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;

/**
 * Created by jixinwang on 2021/9/23
 */
public class ApplierDrcGtidEvent extends MonitoredGtidLogEvent<TransactionContext> {

    public ApplierDrcGtidEvent() {
        super();
        DefaultEventMonitorHolder.getInstance().logBatchEvent("event", "drc gtid", 1, 0);
    }
}
