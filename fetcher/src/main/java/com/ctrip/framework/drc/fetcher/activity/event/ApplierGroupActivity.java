package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.event.ApplierRollbackEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierTransaction;
import com.ctrip.framework.drc.fetcher.event.transaction.BaseBeginEvent;
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
    protected Transaction getTransaction(BaseBeginEvent b) {
        ApplierTransaction transaction = new ApplierTransaction((BeginEvent) b);
        transaction.setTransformerResource(transformerContext);
        return transaction;
    }

    @Override
    protected TransactionEvent getRollbackEvent() {
        return new ApplierRollbackEvent();
    }
}
