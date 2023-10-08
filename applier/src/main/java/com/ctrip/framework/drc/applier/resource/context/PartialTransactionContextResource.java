package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.applier.resource.context.sql.BatchPreparedStatementExecutor;

/**
 * 1、execute batch using BatchPreparedStatementExecutor
 *
 * @Author limingdong
 * @create 2021/2/1
 */
public class PartialTransactionContextResource extends TransactionContextResource implements Batchable {

    private TransactionContextResource parent;

    public PartialTransactionContextResource(TransactionContextResource parent, boolean superBatch) {
        this.parent = parent;
        if (superBatch) {
            this.executor = parent.executor;
        }
    }

    @Override
    public void doInitialize() {
        this.keyValues = parent.getKeyValues();
        this.conflictTableRowsCount = parent.conflictTableRowsCount;
        this.cflRowLogsQueue = parent.cflRowLogsQueue;
        this.logs = parent.logs;
        this.cflTrxLog = parent.cflTrxLog;
        this.trxRowNum = parent.trxRowNum;
        this.conflictRowNum = parent.conflictRowNum;
        this.rollbackRowNum = parent.rollbackRowNum;
        this.lastUnbearable = parent.lastUnbearable;
        this.costTimeNS = parent.costTimeNS;

        this.metricsActivity = parent.metricsActivity;
        this.reportConflictActivity = parent.reportConflictActivity;
        this.progress = parent.progress;
        this.connection = parent.connection;
        this.transactionTable = parent.transactionTable;
    }

    @Override
    public TransactionData.ApplyResult complete() {
        if (everWrong()) {
            return TransactionData.ApplyResult.BATCH_ERROR;
        }
        return doComplete();
    }

    protected TransactionData.ApplyResult doComplete() {
        return ((BatchPreparedStatementExecutor) executor).executeBatch();
    }

    @Override
    public TransactionData.ApplyResult executeBatch() {
        if (everWrong()) {
            return TransactionData.ApplyResult.BATCH_ERROR;
        }
        atTrace("e");
        long start = System.nanoTime();
        TransactionData.ApplyResult result = doExecuteBatch();
        costTimeNS = costTimeNS + (System.nanoTime() - start);
        atTrace("E");
        return result;
    }

    protected TransactionData.ApplyResult doExecuteBatch() {
        return doComplete();
    }
}
