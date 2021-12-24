package com.ctrip.framework.drc.applier.resource.context.savepoint;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.ctrip.framework.drc.applier.resource.context.savepoint.DefaultSavepointExecutor.ROLLBACK_SAVEPOINT;
import static com.ctrip.framework.drc.applier.resource.context.savepoint.DefaultSavepointExecutor.SAVEPOINT;

/**
 * @Author limingdong
 * @create 2021/2/3
 */
public class DefaultSavepointExecutorTest {

    private DefaultSavepointExecutor savepointExecutor;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    private String savepoint = RandomStringUtils.randomAlphabetic(20);

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        savepointExecutor = new DefaultSavepointExecutor(connection);
    }

    @Test
    public void executeSavepoint() throws SQLException {
        String sql = String.format(SAVEPOINT, savepoint);
        Mockito.when(statement.execute(sql)).thenReturn(true);
        boolean res = savepointExecutor.executeSavepoint(savepoint);
        Assert.assertTrue(res);
    }

    @Test
    public void rollbackToSavepoint() throws SQLException {
        String sql = String.format(ROLLBACK_SAVEPOINT, savepoint);
        Mockito.when(statement.execute(sql)).thenReturn(false);
        boolean res = savepointExecutor.rollbackToSavepoint(savepoint);
        Assert.assertFalse(res);
    }
}