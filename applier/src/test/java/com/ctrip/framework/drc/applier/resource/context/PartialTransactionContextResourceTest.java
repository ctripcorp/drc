package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;

/**
 * @Author limingdong
 * @create 2021/2/3
 */
public class PartialTransactionContextResourceTest extends AbstractPartialTransactionContextResource {

    @Test
    public void testDmlCommit() throws SQLException {
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 1});
        Mockito.when(beforeRows.size()).thenReturn(ROW_SIZE * 3);

        partialTransactionContextResource.insert(beforeRows, beforeBitmap, columns);

        TransactionData.ApplyResult applyResult = partialTransactionContextResource.complete();
        Assert.assertEquals(applyResult, TransactionData.ApplyResult.SUCCESS);
    }

    @Test
    public void testDmlRollback() throws SQLException {
        Mockito.when(connection.prepareStatement(Mockito.contains("INSERT"))).thenThrow(sqlException);
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 1});
        Mockito.when(beforeRows.size()).thenReturn(ROW_SIZE * 3);

        partialTransactionContextResource.insert(beforeRows, beforeBitmap, columns);

        TransactionData.ApplyResult applyResult = partialTransactionContextResource.complete();
        Assert.assertEquals(applyResult, TransactionData.ApplyResult.BATCH_ERROR);
    }
}