package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemeCloneTask extends AbstractSchemaTask implements NamedCallable<Boolean> {

    public static final String CREATE_DB = "CREATE DATABASE IF NOT EXISTS %s;";

    public static final String FOREIGN_KEY_CHECKS = "SET FOREIGN_KEY_CHECKS=0";

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
                boolean pass = statement.execute(FOREIGN_KEY_CHECKS);
                DDL_LOGGER.info("[Execute] {} with result {}", FOREIGN_KEY_CHECKS, pass);

                // create database
                for (String dbName : ddlSchemas.keySet()) {
                    if (!addBatch(statement, String.format(CREATE_DB, dbName))) {
                        DDL_LOGGER.info("[Create] database {} in batch", dbName);
                    }
                }
                executeBatch(statement);

                // create table
                for (Map<String, String> tables : ddlSchemas.values()) {
                    for (String tableName : tables.values()) {
                        if (!addBatch(statement, tableName)) {
                            DDL_LOGGER.info("[Create] table {} in batch", tableName);
                        }
                    }
                }
                executeBatch(statement);

                return true;
            }
        }

    }
}
