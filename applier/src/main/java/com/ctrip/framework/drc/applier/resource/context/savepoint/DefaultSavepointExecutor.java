package com.ctrip.framework.drc.applier.resource.context.savepoint;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Author limingdong
 * @create 2021/2/1
 */
public class DefaultSavepointExecutor implements SavepointExecutor {

    public static final String SAVEPOINT = "savepoint %s";

    public static final String ROLLBACK_SAVEPOINT = "rollback to savepoint %s";

    private Connection connection;

    public DefaultSavepointExecutor(Connection connection) {
        this.connection = connection;
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public boolean executeSavepoint(String identifier) throws SQLException {
        String sql = String.format(SAVEPOINT, identifier);
        try (Statement savepointStatement = connection.createStatement()) {
            return savepointStatement.execute(sql);
        }
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public boolean rollbackToSavepoint(String identifier) throws SQLException {
        String sql = String.format(ROLLBACK_SAVEPOINT, identifier);
        try (Statement savepointStatement = connection.createStatement()) {
            return savepointStatement.execute(sql);
        }
    }

}
