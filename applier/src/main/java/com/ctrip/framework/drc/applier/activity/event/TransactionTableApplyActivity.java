package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.fetcher.event.transaction.ApplyTransaction;
import com.ctrip.framework.drc.fetcher.resource.position.TransactionTable;
import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
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
    protected ApplyTransaction onSuccess(ApplyTransaction transaction) throws InterruptedException {
        transactionTable.commit(batch.fetchGtid());
        return super.onSuccess(transaction);
    }

    @Override
    protected ApplyTransaction onDeadLock(ApplyTransaction transaction) throws InterruptedException {
        logger.info("deadlock gtid is: {} for: {}", batch.fetchGtid(), registryKey);
        transactionTable.rollback(batch.fetchGtid());
        return super.onDeadLock(transaction);
    }

    @Override
    protected boolean handleEmptyTransaction(ApplyTransaction transaction) throws InterruptedException {
        if (transaction.isEmptyTransaction()) {
            transaction.reset();
            ApplierGtidEvent event = (ApplierGtidEvent) transaction.next();
            transactionTable.recordToMemory(Gtid.from(event));
            loggerTE.info("({}) record empty transaction to memory", event.getGtid());
            return true;
        }
        return false;
    }
}
