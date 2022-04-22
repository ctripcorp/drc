package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/7
 */
public abstract class BatchTask extends AbstractSchemaTask<Boolean> implements NamedCallable<Boolean> {

    public static int MAX_BATCH_SIZE = 50;

    protected int batchSize = 0;

    protected List<String> sqls = Lists.newArrayList();

    public BatchTask(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        super(inMemoryEndpoint, inMemoryDataSource);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public Boolean call() throws Exception {
        try (Connection connection = inMemoryDataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                beforeExecute(statement);

                for (String sql : sqls) {
                    addBatch(statement, sql);
                }
                executeBatch(statement);

                afterExecute(statement);
            }
        }
        return true;
    }

    protected boolean beforeExecute(Statement statement) throws SQLException {
        return true;
    }

    protected boolean afterExecute(Statement statement) throws SQLException {
        return true;
    }

    protected boolean addBatch(Statement statement, String sql) throws SQLException {
        statement.addBatch(sql);
        DDL_LOGGER.info("[Add] batch {}", sql);
        if (++batchSize >= MAX_BATCH_SIZE) {
            executeBatch(statement);
            return true;
        }
        return false;
    }

    protected void executeBatch(Statement statement) throws SQLException {
        try {
            if (batchSize > 0) {
                DDL_LOGGER.info("[Execute] batch size {} before", batchSize);
                statement.executeBatch();
                DDL_LOGGER.info("[Execute] batch size {} after", batchSize);
                batchSize = 0;
            }
        } catch (Throwable t) {
            DDL_LOGGER.error("[Execute] batch error", t);
            throw t;
        }
    }

}
