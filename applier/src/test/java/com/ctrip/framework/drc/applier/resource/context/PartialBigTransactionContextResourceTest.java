package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;

/**
 * @Author limingdong
 * @create 2021/2/3
 */
public class PartialBigTransactionContextResourceTest extends AbstractPartialTransactionContextResource {

    @Test
    public void testDmlCommit() throws SQLException {
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 1});
        Mockito.when(beforeRows.size()).thenReturn(ROW_SIZE);
        Mockito.when(afterRows.size()).thenReturn(ROW_SIZE);

        /**
         * insert ROW_SIZE, not exec executeBath
         */
        partialTransactionContextResource.insert(beforeRows, beforeBitmap, columns);
        Mockito.verify(statement, Mockito.times(0)).executeBatch();
        Assert.assertEquals(((PartialBigTransactionContextResource)partialTransactionContextResource).getBatchRowsCount(), ROW_SIZE);


        /**
         * update ROW_SIZE, not exec executeBath
         */
        Mockito.when(columns.getBitmapsOfIdentifier()).thenReturn(bitmapsOfIdentifier);

        Mockito.when(columns.getLastBitmapOnUpdate()).thenReturn(bitmapOfIdentifier);
        Mockito.when(bitmapsOfIdentifier.get(Mockito.anyInt())).thenReturn(bitmapOfIdentifier);
        Mockito.when(beforeBitmap.onBitmap(Mockito.any(Bitmap.class))).thenReturn(bitmapOfIdentifier);

        partialTransactionContextResource.update(beforeRows, beforeBitmap, afterRows, afterBitmap, columns);
        Mockito.verify(statement, Mockito.times(0)).executeBatch();
        Assert.assertEquals(((PartialBigTransactionContextResource)partialTransactionContextResource).getBatchRowsCount(), ROW_SIZE * 2);

        /**
         * delete ROW_SIZE, exec executeBath and return success
         */
        partialTransactionContextResource.delete(beforeRows, beforeBitmap, columns);
        Mockito.verify(statement, Mockito.times(1)).executeBatch();
        Assert.assertEquals(((PartialBigTransactionContextResource)partialTransactionContextResource).getBatchRowsCount(), 0);

        partialTransactionContextResource.complete();
    }

    @Test
    public void testDmlConflictCommit() throws SQLException {
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 0});
        Mockito.when(beforeRows.size()).thenReturn(ROW_SIZE * 3);

        /**
         * insert ROW_SIZE, not exec executeBath
         */
        partialTransactionContextResource.insert(beforeRows, beforeBitmap, columns);
        Mockito.verify(statement, Mockito.times(1)).executeBatch();
        Assert.assertEquals(((PartialBigTransactionContextResource)partialTransactionContextResource).getBatchRowsCount(), 0);

        TransactionData.ApplyResult applyResult = partialTransactionContextResource.complete();
        Assert.assertEquals(applyResult, TransactionData.ApplyResult.SUCCESS);
    }

    @Test
    public void testDmlConflictRollback() throws SQLException {
        partialTransactionContextResource.updateGtid(GTID);
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 0});
        Mockito.when(beforeRows.size()).thenReturn(ROW_SIZE * 3);

        Mockito.when(statement.execute(Mockito.contains("rollback to savepoint"))).thenThrow(sqlException);

        /**
         * insert ROW_SIZE, not exec executeBath
         */
        partialTransactionContextResource.insert(beforeRows, beforeBitmap, columns);
        Mockito.verify(statement, Mockito.times(1)).executeBatch();
        Assert.assertEquals(((PartialBigTransactionContextResource)partialTransactionContextResource).getBatchRowsCount(), 0);

        TransactionData.ApplyResult applyResult = partialTransactionContextResource.complete();
        Assert.assertEquals(applyResult, TransactionData.ApplyResult.BATCH_ERROR);
    }

    protected PartialTransactionContextResource getBatchPreparedStatementExecutor(TransactionContextResource parent) {
        return new PartialBigTransactionContextResource(parent);
    }
}