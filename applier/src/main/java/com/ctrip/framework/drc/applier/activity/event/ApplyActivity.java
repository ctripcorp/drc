package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.fetcher.activity.event.BaseApplyActivity;
import com.ctrip.framework.drc.fetcher.event.transaction.ApplyTransaction;
import com.ctrip.framework.drc.applier.resource.context.AccurateTransactionContextResource;
import com.ctrip.framework.drc.applier.resource.context.BatchTransactionContextResource;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * Jul 08, 2020
 */
public class ApplyActivity extends BaseApplyActivity {

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
    public ApplyTransaction doTask(ApplyTransaction transaction) throws InterruptedException {
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
            case CONNECTION_VALIDATE_MASTER_FAILURE:
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.validate.master.fail", registryKey);
                return onFailure(transaction);
            default:
                if (bigTransaction) {
                    logger.error("BIG TRANSACTION error for {}", transaction.identifier());
                    return onFailure(transaction);
                }
                break;
        }

        switch (transaction.apply(accurate)) {
            case COMMUNICATION_FAILURE:
                if (isStopped()) {
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.transaction.table.connect.fail", registryKey);
                    return onFailure(transaction);
                }
                return retry(transaction, 1, TimeUnit.SECONDS);
            case DEADLOCK:
                return onDeadLock(transaction);
            case SUCCESS:
            default:
                return onSuccess(transaction);
        }
    }
}
