package com.ctrip.framework.drc.applier.event.mq;

import com.ctrip.framework.drc.core.mq.DcTag;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqApplierGtidEvent extends ApplierGtidEvent {

    private DcTag dcTag;

    public MqApplierGtidEvent(DcTag dcTag) {
        this.dcTag = dcTag;
    }

    @Override
    protected void beginContext(TransactionContext context) {
        context.updateDcTag(dcTag);
    }
}
