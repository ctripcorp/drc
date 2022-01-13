package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.event.transaction.Transaction;
import com.ctrip.framework.drc.applier.resource.context.AccurateTransactionContextResource;
import com.ctrip.framework.drc.applier.resource.context.BatchTransactionContextResource;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.framework.drc.fetcher.activity.event.EventActivity;

import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * Jul 08, 2020
 */
public class ApplyActivity extends EventActivity<Transaction, Transaction> {

    @InstanceResource
    public DataSource dataSource;

    public BatchTransactionContextResource batch;
    public AccurateTransactionContextResource accurate;

    @Override
    protected void doInitialize() throws Exception {
        batch = (BatchTransactionContextResource) derive(BatchTransactionContextResource.class);
        accurate = (AccurateTransactionContextResource) derive(AccurateTransactionContextResource.class);
    }

    @Override
    public Transaction doTask(Transaction transaction) throws InterruptedException {
        loggerTL.info("apply {}", transaction.identifier());
        boolean bigTransaction = transaction.isOverflowed();
        handleEmptyTransaction(transaction);
        batch.setBigTransaction(bigTransaction);
        switch (transaction.apply(batch)) {
            case SUCCESS:
                return onSuccess(transaction);
            default:
                if (bigTransaction) {
                    logger.error("BIG TRANSACTION error for {}", transaction.identifier());
                    return onFailure(transaction);
                }
                break;
        }

        switch (transaction.apply(accurate)) {
            case COMMUNICATION_FAILURE:
                return retry(transaction, 1, TimeUnit.SECONDS);
            case DEADLOCK:
                return onDeadLock(transaction);
            case SUCCESS:
            default:
                return onSuccess(transaction);
        }
    }

    protected void handleEmptyTransaction(Transaction transaction) throws InterruptedException {
    }

    protected Transaction onSuccess(Transaction transaction) throws InterruptedException {
        return hand(transaction);
    }

    protected Transaction onFailure(Transaction transaction) throws InterruptedException {
        system.mustShutdown();
        return finish(transaction);
    }

    protected Transaction onDeadLock(Transaction transaction) throws InterruptedException {
        return retry(transaction, TimeUnit.SECONDS, 0, 1, 5, 10, (trx) -> {
            logger.error("- UNLIKELY - task ({}) retries exceeds limit.", transaction.identifier());
            return onFailure(trx);
        });
    }
}
