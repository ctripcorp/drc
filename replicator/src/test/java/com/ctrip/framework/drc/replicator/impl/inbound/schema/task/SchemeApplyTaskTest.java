package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemeApplyTask;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask.MAX_RETRY;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemeApplyTaskTest extends AbstractSchemaTaskTest {

    private SchemeApplyTask schemeApplyTask;

    @Mock
    private ExecutorService ddlMonitorExecutorService;

    @Mock
    private BaseEndpointEntity baseEndpointEntity;

    @Mock
    private SQLException sqlException;

    public static final String SCHEMA = "db1.table1";

    public static final String DDL = "/* generated by server */" + "use db1;truncate table table1;";

    @Before
    public void setUp() throws Exception {
        super.initMocks();

        schemeApplyTask = new SchemeApplyTaskMock(inMemoryEndpoint, inMemoryDataSource, SCHEMA, DDL, ddlMonitorExecutorService, baseEndpointEntity);
        retryTask = new RetryTask<>(schemeApplyTask);
    }

    @Test
    public void testExecuteApply() throws Exception {
        when(inMemoryDataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);

        Boolean res = retryTask.call();
        verify(statement, times(2)).execute(anyString());
        Assert.assertTrue(res);
    }

    @Test
    public void testExecuteApplyWithUnknownDatabase() throws Exception {
        when(inMemoryDataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(statement.execute(DDL)).thenThrow(sqlException);
        when(sqlException.getMessage()).thenReturn("Unknown database");

        Boolean res = retryTask.call();
        verify(statement, times(2)).execute(anyString());
        Assert.assertTrue(res);
    }

    @Test
    public void testExecuteApplyWithNoDatabase() throws Exception {
        when(inMemoryDataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(statement.execute(DDL)).thenThrow(sqlException);
        when(sqlException.getMessage()).thenReturn("No database selected");

        Boolean res = retryTask.call();
        verify(statement, times(2)).execute(anyString());
        Assert.assertTrue(res);
    }

    @Test
    public void testExecuteApplyWithException() throws Exception {
        String origin = schemeApplyTask.getDdl();
        String tmpDdl = "truncate table table1;";
        schemeApplyTask.setDdl(tmpDdl);
        when(inMemoryDataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(statement.execute(tmpDdl)).thenThrow(sqlException);
        when(sqlException.getMessage()).thenReturn("test exception");

        Boolean res = retryTask.call();
        schemeApplyTask.setDdl(origin);
        verify(statement, times(2 * (MAX_RETRY + 1))).execute(anyString());
        Assert.assertNull(res);
    }

    class SchemeApplyTaskMock extends SchemeApplyTask {

        public SchemeApplyTaskMock(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource, String schema, String ddl, ExecutorService ddlMonitorExecutorService, BaseEndpointEntity baseEndpointEntity) {
            super(inMemoryEndpoint, inMemoryDataSource, schema, ddl, ddlMonitorExecutorService, baseEndpointEntity);
        }

        @Override
        public void afterException(Throwable t) {
            DDL_LOGGER.warn("[Clear] datasource and recreate for {}", inMemoryEndpoint);
        }
    }
}
