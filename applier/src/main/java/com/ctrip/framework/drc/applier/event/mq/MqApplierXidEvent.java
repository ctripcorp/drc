package com.ctrip.framework.drc.applier.event.mq;

import com.ctrip.framework.drc.fetcher.event.ApplierXidEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;

/**
 * Created by jixinwang on 2022/10/26
 */
public class MqApplierXidEvent extends ApplierXidEvent {

    @Override
    public ApplyResult apply(TransactionContext context) {
        release();
        return TransactionData.ApplyResult.SUCCESS;
    }
}
