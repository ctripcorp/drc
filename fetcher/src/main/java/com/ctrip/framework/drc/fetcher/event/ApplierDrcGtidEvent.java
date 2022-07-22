package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.fetcher.resource.context.EventGroupContext;

/**
 * Created by jixinwang on 2021/9/23
 */
public class ApplierDrcGtidEvent extends ApplierGtidEvent {

    public ApplierDrcGtidEvent() {
        super();
        DefaultEventMonitorHolder.getInstance().logBatchEvent("event", "drc gtid", 1, 0);
    }

    @Override
    public void involve(EventGroupContext context) {
        context.begin(getGtid());
        context.commit();
    }

    @Override
    public TransactionData.ApplyResult apply(TransactionContext context) {
        TransactionData.ApplyResult beginResult = super.apply(context);
        return TransactionData.ApplyResult.SUCCESS == beginResult ? context.complete() : beginResult;
    }
}
