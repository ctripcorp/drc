package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;

public class ApplierRollbackEvent extends ApplierXidEvent {

    @Override
    public ApplyResult terminate(TransactionContext context) {
        loggerED.info(
                "RECONNECT R(" + context.fetchGtid() + ") delay: " + (System.currentTimeMillis() - createdTime) + "ms");
        context.rollback();
        return ApplyResult.SUCCESS;
    }

    @Override
    public void release() {
    }
}
