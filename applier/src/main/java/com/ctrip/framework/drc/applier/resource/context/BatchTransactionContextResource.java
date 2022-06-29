package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.resource.context.sql.BatchPreparedStatementExecutor;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData.ApplyResult;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.framework.drc.fetcher.event.transaction.TransactionData.ApplyResult.*;

/**
 * @Author Slight
 * Apr 19, 2020
 */
public class BatchTransactionContextResource extends TransactionContextResource implements TransactionContext, BigTransactionAware {

    private static final Logger loggerBatch = LoggerFactory.getLogger("BATCH");

    private PartialTransactionContextResource partialTransactionContextResource;

    protected static final int MAX_BATCH_EXECUTE_SIZE = Integer.parseInt(System.getProperty(SystemConfig.MAX_BATCH_EXECUTE_SIZE, "1000"));

    private AtomicLong batchRowsCount = new AtomicLong(0);

    private TransactionData.ApplyResult applyResult = SUCCESS;

    private boolean bigTransaction;

    @Override
    public void doInitialize() throws Exception {
        super.doInitialize();
        executor = new BatchPreparedStatementExecutor(connection.createStatement());
        partialTransactionContextResource = isBigTransaction() ?
                new PartialBigTransactionContextResource(this) :
                new PartialTransactionContextResource(this, true);
        partialTransactionContextResource.initialize();
    }

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        partialTransactionContextResource.insert(beforeRows, beforeBitmap, columns);
        checkBatchExecuteSize(beforeRows.size());
    }

    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        partialTransactionContextResource.update(beforeRows, beforeBitmap, afterRows, afterBitmap, columns);
        checkBatchExecuteSize(beforeRows.size());
    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        partialTransactionContextResource.delete(beforeRows, beforeBitmap, columns);
        checkBatchExecuteSize(beforeRows.size());
    }

    private void checkBatchExecuteSize(int batchSize) {
        loggerBatch.info("[executeBatch] for gtid {}, batchSize {}, size {}", fetchGtid(), batchSize, batchRowsCount.get());
        if (batchRowsCount.addAndGet(batchSize) >= MAX_BATCH_EXECUTE_SIZE) {
            try {
                if (SUCCESS == applyResult) {
                    applyResult = partialTransactionContextResource.executeBatch();
                    loggerBatch.info("[executeBatch] for gtid {}, size {}", fetchGtid(), batchRowsCount.get());
                } else {
                    loggerBatch.error("[executeBatch] skip for applyResult {}", applyResult);
                }
            } finally {
                batchRowsCount.set(0);
            }
        }
    }

    @Override
    public void recordTransactionTable(String gtid) {
        partialTransactionContextResource.recordTransactionTable(gtid);
    }

    @Override
    public ApplyResult complete() {
        boolean whateverGoesWrong = false;
        if (SUCCESS != applyResult || BATCH_ERROR == partialTransactionContextResource.complete() || everWrong()) {
            whateverGoesWrong = true;
        }

        setLastUnbearable(null);

        if (whateverGoesWrong) {
            rollback();
            conflictAndRollback();
            if (getLastUnbearable() != null) {
                return UNKNOWN;
            } else {
                return SUCCESS != applyResult ? applyResult : WHATEVER_ROLLBACK;
            }
        }
        commit();
        if (getConflictMap().contains(true)) {
            conflictAndCommit();
        }
        if (getLastUnbearable() != null) {
            return UNKNOWN;
        } else {
            return SUCCESS;
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

    public boolean isBigTransaction() {
        return bigTransaction;
    }

    @Override
    public void beginTransactionTable(String gtid) {
        try {
            transactionTable.begin(gtid);
        } catch (InterruptedException e) {
            logger.error("set transaction table begin statue error,exception is: {}", e.getCause().toString());
        }
    }

    @Override
    protected String contextDesc() {
        return bigTransaction ? " BIG" : " BATCH";
    }

    @VisibleForTesting
    protected long getBatchRowsCount() {
        return batchRowsCount.get();
    }
}
