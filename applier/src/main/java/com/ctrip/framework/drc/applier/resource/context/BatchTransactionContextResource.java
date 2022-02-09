package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData.ApplyResult;
import com.ctrip.framework.drc.applier.resource.context.sql.BatchPreparedStatementExecutor;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;

import java.util.List;

import static com.ctrip.framework.drc.fetcher.event.transaction.TransactionData.ApplyResult.BATCH_ERROR;

/**
 * @Author Slight
 * Apr 19, 2020
 */
public class BatchTransactionContextResource extends TransactionContextResource implements TransactionContext, BigTransactionAware {

    private PartialTransactionContextResource partialTransactionContextResource;

    private boolean bigTransaction;

    @Override
    public void doInitialize() throws Exception {
        super.doInitialize();
        executor = new BatchPreparedStatementExecutor(connection.createStatement());
        partialTransactionContextResource = bigTransaction ?
                new PartialBigTransactionContextResource(this) :
                new PartialTransactionContextResource(this, true);
        partialTransactionContextResource.initialize();
    }

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        partialTransactionContextResource.insert(beforeRows, beforeBitmap, columns);
    }

    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        partialTransactionContextResource.update(beforeRows, beforeBitmap, afterRows, afterBitmap, columns);
    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        partialTransactionContextResource.delete(beforeRows, beforeBitmap, columns);
    }

    @Override
    public void recordTransactionTable(String gtid) {
        partialTransactionContextResource.recordTransactionTable(gtid);
    }

    @Override
    public ApplyResult complete() {
        boolean whateverGoesWrong = false;
        if (BATCH_ERROR == partialTransactionContextResource.complete() || everWrong()) {
            whateverGoesWrong = true;
        }

        setLastUnbearable(null);

        if (whateverGoesWrong) {
            rollback();
            conflictAndRollback();
            if (getLastUnbearable() != null) {
                return ApplyResult.UNKNOWN;
            } else {
                return ApplyResult.WHATEVER_ROLLBACK;
            }
        }
        commit();
        if (getConflictMap().contains(true)) {
            conflictAndCommit();
        }
        if (getLastUnbearable() != null) {
            return ApplyResult.UNKNOWN;
        } else {
            return ApplyResult.SUCCESS;
        }
    }

    public void doDispose() {
        ((BatchPreparedStatementExecutor) executor).dispose();
        super.doDispose();
    }

    @Override
    public void setBigTransaction(boolean bigTransaction) {
        this.bigTransaction = bigTransaction;
    }

    @Override
    public void beginTransactionTable(String gtid) {
        try {
            transactionTable.begin(gtid);
        } catch (InterruptedException e) {
            logger.error("set transaction table begin statue error,exception is: {}", e.getCause().toString());
        }
    }
}
