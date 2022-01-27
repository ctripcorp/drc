package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.event.ApplierRollbackEvent;
import com.ctrip.framework.drc.applier.event.ApplierTransaction;
import com.ctrip.framework.drc.fetcher.activity.event.GroupActivity;
import com.ctrip.framework.drc.fetcher.event.transaction.BeginEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.Transaction;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.transformer.TransformerContext;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

/**
 * @Author limingdong
 * @create 2021/3/4
 */
public class ApplierGroupActivity extends GroupActivity {

    @InstanceResource
    public TransformerContext transformerContext;

    @Override
    protected Transaction getTransaction(BeginEvent b) {
        ApplierTransaction transaction = new ApplierTransaction((com.ctrip.framework.drc.applier.event.transaction.BeginEvent) b);
        transaction.setTransformerResource(transformerContext);
        return transaction;
    }

    @Override
    protected TransactionEvent getRollbackEvent() {
        return new ApplierRollbackEvent();
    }
}
