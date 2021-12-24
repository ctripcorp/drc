package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.event.transaction.Transaction;
import com.ctrip.framework.drc.applier.resource.TransactionTable;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

/**
 * Created by jixinwang on 2021/8/25
 */
public class TransactionTableApplyActivity extends ApplyActivity {

    @InstanceResource
    public TransactionTable transactionTable;

    @Override
    protected Transaction onSuccess(Transaction transaction) throws InterruptedException {
        transactionTable.commit(batch.fetchGtid());
        return super.onSuccess(transaction);
    }

    @Override
    protected Transaction onDeadLock(Transaction transaction) throws InterruptedException {
        logger.info("deadlock gtid is: " + batch.fetchGtid());
        transactionTable.rollback(batch.fetchGtid());
        return super.onDeadLock(transaction);
    }
}
