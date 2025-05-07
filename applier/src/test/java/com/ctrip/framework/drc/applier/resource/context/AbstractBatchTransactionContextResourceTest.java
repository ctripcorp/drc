package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.fetcher.resource.context.TransactionContextResource;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;

/**
 * @Author limingdong
 * @create 2022/6/28
 */
public abstract class AbstractBatchTransactionContextResourceTest extends AbstractPartialTransactionContextResource {

    @Test
    public void testDmlCommit() throws SQLException {
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 1});
        Mockito.when(beforeRows.size()).thenReturn(ROW_SIZE);
        Mockito.when(afterRows.size()).thenReturn(ROW_SIZE);

        /**
         * insert ROW_SIZE, not exec executeBath
         */
        transactionContextResource.insert(beforeRows, beforeBitmap, columns);
        Mockito.verify(statement, Mockito.times(0)).executeBatch();
        Assert.assertEquals(((BatchTransactionContextResource) transactionContextResource).getBatchRowsCount(), ROW_SIZE);


        /**
         * update ROW_SIZE, not exec executeBath
         */
        Mockito.when(columns.getBitmapsOfIdentifier()).thenReturn(bitmapsOfIdentifier);

        Mockito.when(columns.getLastBitmapOnUpdate()).thenReturn(bitmapOfIdentifier);
        Mockito.when(bitmapsOfIdentifier.get(Mockito.anyInt())).thenReturn(bitmapOfIdentifier);
        Mockito.when(beforeBitmap.onBitmap(Mockito.any(Bitmap.class))).thenReturn(bitmapOfIdentifier);

        transactionContextResource.update(beforeRows, beforeBitmap, afterRows, afterBitmap, columns);
        Mockito.verify(statement, Mockito.times(0)).executeBatch();
        Assert.assertEquals(((BatchTransactionContextResource) transactionContextResource).getBatchRowsCount(), ROW_SIZE * 2);

        /**
         * delete ROW_SIZE, exec executeBath and return success
         */
        transactionContextResource.delete(beforeRows, beforeBitmap, columns);
        Mockito.verify(statement, Mockito.times(1)).executeBatch();
        Assert.assertEquals(((BatchTransactionContextResource) transactionContextResource).getBatchRowsCount(), 0);

        transactionContextResource.complete();
    }

    @Override
    protected TransactionContextResource getBatchPreparedStatementExecutor(ApplierTransactionContextResource parent) {
        return parent;
    }
}
