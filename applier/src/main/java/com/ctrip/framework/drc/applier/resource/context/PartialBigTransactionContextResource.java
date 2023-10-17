package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.resource.context.savepoint.DefaultSavepointExecutor;
import com.ctrip.framework.drc.applier.resource.context.savepoint.SavepointExecutor;
import com.ctrip.framework.drc.applier.resource.context.sql.BatchPreparedStatementExecutor;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 1、execute batch using BatchPreparedStatementExecutor
 * 2、execute sql by sql after conflict because of batch execution using OneRowPreparedStatementExecutor
 *
 * @Author limingdong
 * @create 2021/2/1
 */
public class PartialBigTransactionContextResource extends PartialTransactionContextResource {

    private static final Logger loggerBatch = LoggerFactory.getLogger("BATCH");

    private SavepointExecutor savepointExecutor;

    private List<Runnable> writeEventWrappers = Lists.newArrayList();

    private BatchPreparedStatementExecutor preparedStatementExecutor;

    public PartialBigTransactionContextResource(TransactionContextResource parent) {
        super(parent, false);
        this.preparedStatementExecutor = (BatchPreparedStatementExecutor) parent.executor;
    }

    @Override
    public void doInitialize() {
        super.doInitialize();
        savepointExecutor = new DefaultSavepointExecutor(connection);
        batchRowsCount.set(0);
    }

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        super.insert(beforeRows, beforeBitmap, columns, preparedStatementExecutor);
        TableKey tableKey = fetchTableKey();
        writeEventWrappers.add(() -> {
            setTableKey(tableKey);
            super.insert(beforeRows, beforeBitmap, columns);
            trxRowNum.subtract(beforeRows.size());
        });
        batchRowsCount.addAndGet(beforeRows.size());
    }

    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        super.update(beforeRows, beforeBitmap, afterRows, afterBitmap, columns, preparedStatementExecutor);
        TableKey tableKey = fetchTableKey();
        writeEventWrappers.add(() -> {
            setTableKey(tableKey);
            super.update(beforeRows, beforeBitmap, afterRows, afterBitmap, columns);
            trxRowNum.subtract(beforeRows.size());
        });
        batchRowsCount.addAndGet(beforeRows.size());
    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        super.delete(beforeRows, beforeBitmap, columns, preparedStatementExecutor);
        TableKey tableKey = fetchTableKey();
        writeEventWrappers.add(() -> {
            setTableKey(tableKey);
            super.delete(beforeRows, beforeBitmap, columns);
            trxRowNum.subtract(beforeRows.size()); // 状态回滚放到rollback
        });
        batchRowsCount.addAndGet(beforeRows.size());
    }

    protected TransactionData.ApplyResult doExecuteBatch() {
        try {
            String savepointIdentifier = String.format("%d_%d", fetchSequenceNumber(), System.currentTimeMillis());
            savepointExecutor.executeSavepoint(savepointIdentifier);
            loggerBatch.info("[Savepoint] execute for {}", savepointIdentifier);
            switch (preparedStatementExecutor.executeBatch()) {
                case SUCCESS: //no need to release savepoint
                    loggerBatch.info("[executeBatch] SUCCESS for {}", fetchGtid());
                    return TransactionData.ApplyResult.SUCCESS;
                case BATCH_ERROR:
                    loggerBatch.info("[executeBatch] BATCH_ERROR for {}", fetchGtid());
                    return conflictHandling(savepointIdentifier); 
            }
        } catch (Throwable t) {
            logger.error("executeBatch error for {}", fetchGtid(), t);
            setLastUnbearable(t);  // skip next insert、update and delete for lastUnbearable not null
        } finally {
            batchRowsCount.set(0);
            writeEventWrappers.clear();
        }

        return TransactionData.ApplyResult.BATCH_ERROR;
    }

    private TransactionData.ApplyResult conflictHandling(String savepointIdentifier) throws SQLException {
        savepointExecutor.rollbackToSavepoint(savepointIdentifier);
        loggerBatch.info("[Savepoint] rollback for {}", savepointIdentifier);
        writeEventWrappers.forEach(Runnable::run);
        boolean handleResult = everWrong();
        loggerBatch.info("[conflictHandling] {} for {} and lastUnbearable is {}", handleResult, fetchGtid(), getLastUnbearable());
        return handleResult ? TransactionData.ApplyResult.BATCH_ERROR : TransactionData.ApplyResult.SUCCESS;
    }

    @Override
    protected TransactionData.ApplyResult doComplete() {
        return executeBatch();
    }

    @VisibleForTesting
    private AtomicLong batchRowsCount = new AtomicLong(0);

    @VisibleForTesting
    protected long getBatchRowsCount() {
        return batchRowsCount.get();
    }

}