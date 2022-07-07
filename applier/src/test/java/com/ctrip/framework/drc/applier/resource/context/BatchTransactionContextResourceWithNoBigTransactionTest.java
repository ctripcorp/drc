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
public class BatchTransactionContextResourceWithNoBigTransactionTest extends AbstractBatchTransactionContextResourceTest {

    @Test
    public void testDmlConflictRollback() throws SQLException {
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 0});
        Mockito.when(beforeRows.size()).thenReturn(ROW_SIZE * 3);

        /**
         * insert ROW_SIZE, not exec executeBath
         */
        transactionContextResource.insert(beforeRows, beforeBitmap, columns);
        Mockito.verify(statement, Mockito.times(1)).executeBatch();
        Assert.assertEquals(((BatchTransactionContextResource) transactionContextResource).getBatchRowsCount(), 0);

        TransactionData.ApplyResult applyResult = transactionContextResource.complete();
        Assert.assertEquals(applyResult, TransactionData.ApplyResult.BATCH_ERROR);
        Mockito.verify(statement, Mockito.times(1)).executeBatch();
    }

}
