package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.applier.resource.context.TransactionContext;

public class ApplierRollbackEvent extends ApplierXidEvent {

    @Override
    public ApplyResult terminate0(TransactionContext context) {
        loggerED.info(
                "RECONNECT R(" + context.fetchGtid() + ") delay: " + (System.currentTimeMillis() - createdTime) + "ms");
        context.rollback();
        return ApplyResult.SUCCESS;
    }

    @Override
    public void release() {
    }
}
