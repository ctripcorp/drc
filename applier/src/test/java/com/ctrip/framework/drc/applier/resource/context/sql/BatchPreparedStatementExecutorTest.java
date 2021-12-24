package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Author limingdong
 * @create 2021/2/3
 */
public class BatchPreparedStatementExecutorTest {

    private BatchPreparedStatementExecutor batchPreparedStatementExecutor;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private PreparedStatement preparedStatement;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.when(connection.createStatement()).thenReturn(statement);

        batchPreparedStatementExecutor = new BatchPreparedStatementExecutor(statement);
    }

    @After
    public void tearDown() {
        batchPreparedStatementExecutor.dispose();
    }

    @Test
    public void executeSuccess() throws SQLException {
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 1});
        batchPreparedStatementExecutor.execute(preparedStatement);
        TransactionData.ApplyResult applyResult = batchPreparedStatementExecutor.executeBatch();
        Assert.assertEquals(applyResult, TransactionData.ApplyResult.SUCCESS);
    }

    @Test
    public void executeError() throws SQLException {
        Mockito.when(statement.executeBatch()).thenReturn(new int[]{1, 0});
        batchPreparedStatementExecutor.execute(preparedStatement);
        TransactionData.ApplyResult applyResult = batchPreparedStatementExecutor.executeBatch();
        Assert.assertEquals(applyResult, TransactionData.ApplyResult.BATCH_ERROR);
    }
}