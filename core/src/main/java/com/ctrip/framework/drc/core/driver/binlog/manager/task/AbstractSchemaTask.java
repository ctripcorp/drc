package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public abstract class AbstractSchemaTask implements NamedCallable<Boolean> {

    protected Endpoint inMemoryEndpoint;

    protected DataSource inMemoryDataSource;

    protected int MAX_BATCH_SIZE = 10;

    protected int batchSize = 0;

    public AbstractSchemaTask(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        this.inMemoryEndpoint = inMemoryEndpoint;
        this.inMemoryDataSource = inMemoryDataSource;
    }

    @Override
    public void afterException(Throwable t) {
        NamedCallable.super.afterException(t);
        DataSourceManager.getInstance().clearDataSource(inMemoryEndpoint);
        inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint);
        NamedCallable.DDL_LOGGER.warn("[Clear] datasource and recreate for {}", inMemoryEndpoint);
    }

    protected boolean addBatch(Statement statement, String sql) throws SQLException {
        statement.addBatch(sql);
        if (++batchSize >= MAX_BATCH_SIZE) {
            executeBatch(statement);
            return true;
        }
        return false;
    }

    protected void executeBatch(Statement statement) throws SQLException {
        if (batchSize > 0) {
            statement.executeBatch();
            batchSize = 0;
            DDL_LOGGER.info("[Execute] batch");
        }
    }
}
