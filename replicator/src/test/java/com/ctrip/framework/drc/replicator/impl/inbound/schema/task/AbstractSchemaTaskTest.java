package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask.MAX_RETRY;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class AbstractSchemaTaskTest extends MockTest {

    @Mock
    protected DataSource inMemoryDataSource;

    @Mock
    protected Connection connection;

    @Mock
    protected SQLException sqlException;

    protected RetryTask<Boolean> retryTask;

    @Mock
    protected Endpoint inMemoryEndpoint;

    @Mock
    protected Statement statement;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
    }

    @Test
    public void testConnectionException() throws Exception {
        when(inMemoryDataSource.getConnection()).thenThrow(sqlException);
        Boolean res = retryTask.call();
        verify(inMemoryDataSource, times((MAX_RETRY + 1) * 2)).getConnection();  /*comment query*/
        Assert.assertNull(res);
    }

    @Test
    public void testCreateStatementException() throws Exception {
        when(inMemoryDataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenThrow(sqlException);

        Boolean res = retryTask.call();
        verify(inMemoryDataSource, times((MAX_RETRY + 1) * 2)).getConnection(); /*comment query*/
        verify(connection, times((MAX_RETRY + 1) * 2)).createStatement(); /*comment query*/
        Assert.assertNull(res);
    }
}
