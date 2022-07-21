package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;

/**
 * Created by jixinwang on 2021/8/20
 */
public class TransactionTableApplierGtidEvent extends ApplierGtidEvent {

    public TransactionTableApplierGtidEvent() {
        super();
    }

    @Override
    protected void beginContext(TransactionContext context) {
        context.begin();
        context.beginTransactionTable(getGtid());
        context.recordTransactionTable(getGtid());
    }
}
