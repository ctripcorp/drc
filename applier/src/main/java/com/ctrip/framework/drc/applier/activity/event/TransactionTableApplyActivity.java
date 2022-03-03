package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.event.ApplierGtidEvent;
import com.ctrip.framework.drc.applier.event.transaction.Transaction;
import com.ctrip.framework.drc.applier.resource.position.TransactionTable;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jixinwang on 2021/8/25
 */
public class TransactionTableApplyActivity extends ApplyActivity {

    private final Logger loggerTE = LoggerFactory.getLogger("TRX END");

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

    @Override
    protected boolean handleEmptyTransaction(Transaction transaction) throws InterruptedException {
        if (transaction.isEmptyTransaction()) {
            transaction.reset();
            ApplierGtidEvent event = (ApplierGtidEvent)transaction.next();
            transactionTable.recordToMemory(event.getGtid());
            loggerTE.info("({}) record empty transaction to memory", event.getGtid());
            return true;
        }
        return false;
    }
}
