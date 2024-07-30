package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.event.transaction.Transaction;
import com.ctrip.framework.drc.applier.resource.context.AccurateTransactionContextResource;
import com.ctrip.framework.drc.applier.resource.context.BatchTransactionContextResource;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.activity.event.EventActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;

import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * Jul 08, 2020
 */
public class ApplyActivity extends EventActivity<Transaction, Transaction> {

    @InstanceResource
    public DataSource dataSource;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    public BatchTransactionContextResource batch;
    public AccurateTransactionContextResource accurate;

    @Override
    protected void doInitialize() throws Exception {
        batch = (BatchTransactionContextResource) derive(BatchTransactionContextResource.class);
        accurate = (AccurateTransactionContextResource) derive(AccurateTransactionContextResource.class);
    }

    @Override
    public Transaction doTask(Transaction transaction) throws InterruptedException {
        loggerTL.info("[{}] apply {}", registryKey, transaction.identifier());
        boolean bigTransaction = transaction.isOverflowed();
        if (handleEmptyTransaction(transaction)) {
            return hand(transaction);
        }
        batch.setBigTransaction(bigTransaction);
        switch (transaction.apply(batch)) {
            case SUCCESS:
                return onSuccess(transaction);
            case TRANSACTION_TABLE_CONFLICT_ROLLBACK:
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.transaction.table.conflict", registryKey);
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

    protected boolean handleEmptyTransaction(Transaction transaction) throws InterruptedException {
        return false;
    }

    protected Transaction onSuccess(Transaction transaction) throws InterruptedException {
        return hand(transaction);
    }

    protected Transaction onFailure(Transaction transaction) throws InterruptedException {
        logger.error("apply failed: {}, shutdown server", registryKey);
        logger.info("apply activity status is stopped for {}", registryKey);
        getSystem().setStatus(SystemStatus.STOPPED);
        return finish(transaction);
    }

    protected Transaction onDeadLock(Transaction transaction) throws InterruptedException {
        return retry(transaction, TimeUnit.SECONDS, 0, 1, 5, 10, (trx) -> {
            logger.error("- UNLIKELY - task ({}) retries exceeds limit.", transaction.identifier());
            return onFailure(trx);
        });
    }
}
