package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.applier.resource.context.sql.BatchPreparedStatementExecutor;

/**
 * 1、execute batch using BatchPreparedStatementExecutor
 * @Author limingdong
 * @create 2021/2/1
 */
public class PartialTransactionContextResource extends TransactionContextResource {

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
        this.conflictMap = parent.conflictMap;
        this.overwriteMap = parent.overwriteMap;
        this.logs = parent.logs;
        this.conflictTransactionLog = parent.conflictTransactionLog;
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
        return ((BatchPreparedStatementExecutor)executor).executeBatch();
    }
}
