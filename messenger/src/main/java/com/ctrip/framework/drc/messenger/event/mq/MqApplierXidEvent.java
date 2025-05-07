package com.ctrip.framework.drc.messenger.event.mq;

import com.ctrip.framework.drc.fetcher.event.ApplierXidEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;

/**
 * Created by jixinwang on 2022/10/26
 */
public class MqApplierXidEvent extends ApplierXidEvent {

    @Override
    public ApplyResult apply(TransactionContext context) {
        release();
        return terminate(context);
    }
}
