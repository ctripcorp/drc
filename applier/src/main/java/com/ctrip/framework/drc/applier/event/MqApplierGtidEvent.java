package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqApplierGtidEvent extends ApplierGtidEvent {

    @Override
    protected void beginContext(TransactionContext context) {

    }
}
