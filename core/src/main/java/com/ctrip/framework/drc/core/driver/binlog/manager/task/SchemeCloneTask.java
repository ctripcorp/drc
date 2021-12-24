package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemeCloneTask extends AbstractSchemaTask implements NamedCallable<Boolean> {

    public static final String CREATE_DB = "CREATE DATABASE IF NOT EXISTS %s;";

    private Map<String, Map<String, String>> ddlSchemas;

    public SchemeCloneTask(Map<String, Map<String, String>> ddlSchemas, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        super(inMemoryEndpoint, inMemoryDataSource);
        this.ddlSchemas = ddlSchemas;
    }

    @Override
    public void afterException(Throwable t) {
        super.afterException(t);
        new RetryTask<>(new SchemeClearTask(inMemoryEndpoint, inMemoryDataSource)).call();
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public Boolean call() throws Exception {
        try (Connection connection = inMemoryDataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                for (Map.Entry<String, Map<String, String>> entry : ddlSchemas.entrySet()) {
                    List<String> foreignKey = Lists.newArrayList();
                    boolean pass = statement.execute(String.format(CREATE_DB, entry.getKey()));
                    DDL_LOGGER.info("[Create] database {} with result {}", entry.getKey(), pass);
                    Map<String, String> sqls = entry.getValue();
                    int batchSize = 0;
                    for (String sql : sqls.values()) {
                        if (sql.contains("FOREIGN KEY")) {
                            foreignKey.add(sql);
                            DDL_LOGGER.info("FOREIGN KEY sql is {}", sql);
                        } else {
                            statement.addBatch(sql);
                            batchSize++;
                            if (batchSize >= 10) {
                                statement.executeBatch();
                                batchSize = 0;
                                DDL_LOGGER.info("[Create] batch");
                            }
                            DDL_LOGGER.info("[Create] table {}", sql);
                        }
                    }

                    if (batchSize > 0) {
                        statement.executeBatch();
                    }

                    List<String> error = Lists.newArrayList();
                    for (String f : foreignKey) {
                        try {
                            statement.execute(f);
                        } catch (Exception e) {
                            DDL_LOGGER.error("execute foreign key error for {}", f, e);
                            error.add(f);
                        }
                    }

                    if (!error.isEmpty()) {
                        for (int i = error.size() - 1; i >= 0; i--) {
                            statement.execute(error.get(i));
                        }
                    }
                }
                return true;
            }
        }

    }
}
