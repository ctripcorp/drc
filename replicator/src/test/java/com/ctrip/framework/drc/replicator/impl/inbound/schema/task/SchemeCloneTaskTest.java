package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemeCloneTask;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemeCloneTask.FOREIGN_KEY_CHECKS;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemeCloneTaskTest extends AbstractSchemaTaskTest {

    private SchemeCloneTask schemeCloneTask;

    private Map<String, Map<String, String>> ddlSchemas = Maps.newHashMap();

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        Map<String, String> table1 = Maps.newHashMap(); //batch 1 times
        table1.put("table1", "create table1");
        table1.put("table2", "create table1 FOREIGN KEY");
        ddlSchemas.put("db1", table1);

        Map<String, String> table2 = Maps.newHashMap();  //batch 2 times
        table2.put("table2", "create table2");
        table2.put("table3", "create table3");
        table2.put("table4", "create table4");
        table2.put("table5", "create table5");
        table2.put("table6", "create table6");
        table2.put("table7", "create table7");
        table2.put("table8", "create table8");
        table2.put("table9", "create table9");
        table2.put("table10", "create table10");
        table2.put("table11", "create table11");
        table2.put("table12", "create table12");
        table2.put("table13", "create table13");
        ddlSchemas.put("db2", table2);

        schemeCloneTask = new SchemeCloneTaskMock(ddlSchemas, inMemoryEndpoint, inMemoryDataSource);
        retryTask = new RetryTask<>(schemeCloneTask);
    }

    @Test
    public void testExecuteBatch() throws Exception {
        when(inMemoryDataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenThrow(sqlException);

        Boolean res = retryTask.call();
        verify(statement, times(3)).executeBatch();
        verify(statement, times(0)).execute(Mockito.contains("FOREIGN KEY")); // executeBatch instead
        verify(statement, times(1)).execute(FOREIGN_KEY_CHECKS); // executeBatch instead
        Assert.assertTrue(res);
    }

    class SchemeCloneTaskMock extends SchemeCloneTask {

        public SchemeCloneTaskMock(Map<String, Map<String, String>> ddlSchemas, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
            super(ddlSchemas, inMemoryEndpoint, inMemoryDataSource);
        }

        @Override
        public void afterException(Throwable t) {
            DDL_LOGGER.warn("[Clear] datasource and recreate for {}", inMemoryEndpoint);
        }
    }
}
