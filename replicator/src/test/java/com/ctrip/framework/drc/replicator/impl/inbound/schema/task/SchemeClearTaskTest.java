package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemeClearTask;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask.MAX_RETRY;
import static com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemeClearTask.DB_NOT_EXIST;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemeClearTaskTest extends AbstractSchemaTaskTest {

    private SchemeClearTask schemeClearTask;

    private ResultSet resultSet;

    @Mock
    private SQLException dbException;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        schemeClearTask = new SchemeClearTaskMock(inMemoryEndpoint, inMemoryDataSource);
        retryTask = new RetryTask<>(schemeClearTask);
        resultSet = ResultSetMock.create(new String[]{"id", "schema", "table"}, new Object[][]{{1, "db1", "table1"}, {2, "db2", "table2"}});
    }

    @Test
    public void testExecuteQueryException() throws Exception {
        when(inMemoryDataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenThrow(sqlException);

        Boolean res = retryTask.call();
        verify(inMemoryDataSource, times(MAX_RETRY + 1)).getConnection();
        verify(connection, times(MAX_RETRY + 1)).createStatement();
        verify(statement, times(MAX_RETRY + 1)).executeQuery(anyString());
        Assert.assertNull(res);
    }

    @Test
    public void testExecuteException() throws Exception {
        when(inMemoryDataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(statement.execute(Mockito.contains("db2"))).thenThrow(dbException);
        when(statement.execute(Mockito.contains("db1"))).thenReturn(true);
        when(dbException.getMessage()).thenReturn(DB_NOT_EXIST);

        Boolean res = retryTask.call();
        verify(inMemoryDataSource, times(1)).getConnection();
        verify(connection, times( 1)).createStatement();
        verify(statement, times( 1)).executeQuery(anyString());
        verify(statement, times( 2)).execute(anyString());
        Assert.assertTrue(res.booleanValue());
    }

    class SchemeClearTaskMock extends SchemeClearTask {

        public SchemeClearTaskMock(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
            super(inMemoryEndpoint, inMemoryDataSource);
        }

        @Override
        public void afterException(Throwable t) {
            DDL_LOGGER.warn("[Clear] datasource and recreate for {}", inMemoryEndpoint);
        }
    }
}
