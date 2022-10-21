package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.event.transaction.Transaction;
import com.ctrip.framework.drc.applier.mq.MqPositionResource;
import com.ctrip.framework.drc.applier.resource.context.MqTransactionContextResource;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqApplyActivity extends ApplyActivity {

    public MqTransactionContextResource transactionContext;

    @InstanceResource
    public MqPositionResource mqPositionResource;

    @Override
    protected void doInitialize() throws Exception {
        transactionContext = (MqTransactionContextResource) derive(MqTransactionContextResource.class);
    }

    @Override
    public Transaction doTask(Transaction transaction) throws InterruptedException {
        loggerTL.info("[{}] apply {}", registryKey, transaction.identifier());

        switch (transaction.apply(transactionContext)) {
            case SUCCESS:
                return onSuccess(transaction);
            default:
                return onFailure(transaction);
        }
    }

    @Override
    protected Transaction onSuccess(Transaction transaction) throws InterruptedException {
//        mqPositionResource.update(transactionContext.fetchGtid());
        return super.onSuccess(transaction);
    }
}
