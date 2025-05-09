package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaExtractor;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.ctrip.framework.drc.core.AllTests.*;
import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.getDefaultPoolProperties;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;

/**
 * @Author limingdong
 * @create 2022/4/7
 */
public abstract class AbstractSchemaTest<T> {

    protected AbstractSchemaTask<T> abstractSchemaTask;

    protected Endpoint inMemoryEndpoint;

    protected DataSource inMemoryDataSource;

    @Before
    public void setUp() throws Exception {
        inMemoryEndpoint = new DefaultEndPoint(IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD);
        PoolProperties poolProperties = getDefaultPoolProperties(inMemoryEndpoint);
        String timeout = String.format("connectTimeout=%s;socketTimeout=%s", CONNECTION_TIMEOUT, 60);
        poolProperties.setConnectionProperties(timeout);
        inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint, poolProperties);
        abstractSchemaTask = getAbstractSchemaTask();
    }


    protected List<String> query(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                return SchemaExtractor.extractValues(resultSet, null);
            }
        }
    }

    protected void assertResultSize(Connection connection, String dbName, int size) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("use " + dbName);
            try (ResultSet resultSet = statement.executeQuery("SHOW TABLES;")) {
                List<String> tableNames = SchemaExtractor.extractValues(resultSet, null);
                Assert.assertTrue(tableNames.size() == size);
            }
        }
    }

    protected abstract AbstractSchemaTask<T> getAbstractSchemaTask();
}
