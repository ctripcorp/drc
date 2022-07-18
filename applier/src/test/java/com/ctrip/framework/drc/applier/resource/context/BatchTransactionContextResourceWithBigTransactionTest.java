package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;

/**
 * @Author limingdong
 * @create 2022/6/28
 */
public class BatchTransactionContextResourceWithBigTransactionTest extends AbstractBatchTransactionContextResourceTest {

    @Test
    public void testDmlConflictCommit() throws SQLException {
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 0});
        Mockito.when(beforeRows.size()).thenReturn(ROW_SIZE * 3);

        /**
         * insert ROW_SIZE, not exec executeBath
         */
        transactionContextResource.insert(beforeRows, beforeBitmap, columns);
        if (transactionContextResource instanceof Batchable) {
            ((Batchable)transactionContextResource).executeBatch();
        }
        Mockito.verify(statement, Mockito.times(1)).executeBatch();
        Assert.assertEquals(((BatchTransactionContextResource) transactionContextResource).getBatchRowsCount(), 0);

        TransactionData.ApplyResult applyResult = transactionContextResource.complete();
        Assert.assertEquals(applyResult, TransactionData.ApplyResult.SUCCESS);
    }

    @Test
    public void testDmlConflictRollback() throws SQLException {
        transactionContextResource.updateGtid(GTID);
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 0});
        Mockito.when(beforeRows.size()).thenReturn(ROW_SIZE * 3);

        Mockito.when(statement.execute(Mockito.contains("rollback to savepoint"))).thenThrow(sqlException);

        /**
         * insert ROW_SIZE, not exec executeBath
         */
        transactionContextResource.insert(beforeRows, beforeBitmap, columns);
        if (transactionContextResource instanceof Batchable) {
            ((Batchable)transactionContextResource).executeBatch();
        }
        Mockito.verify(statement, Mockito.times(1)).executeBatch();
        Assert.assertEquals(((BatchTransactionContextResource) transactionContextResource).getBatchRowsCount(), 0);

        TransactionData.ApplyResult applyResult = transactionContextResource.complete();
        Assert.assertEquals(applyResult, TransactionData.ApplyResult.BATCH_ERROR);
    }

    @Override
    protected boolean bigTransaction() {
        return true;
    }
}
