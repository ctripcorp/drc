package com.ctrip.framework.drc.core.driver.pool;

import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.PoolConfiguration;
import org.apache.tomcat.jdbc.pool.PooledConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.isIntegrityTest;

/**
 * Created by jixinwang on 2020/12/4
 */
public class DrcConnectionPool extends ConnectionPool {

    private static Logger logger = LoggerFactory.getLogger(DrcConnectionPool.class);

    public static int SESSION_WAIT_TIMEOUT = 120;

    public DrcConnectionPool(PoolConfiguration prop) throws SQLException {
        super(prop);
    }

    @Override
    protected PooledConnection borrowConnection(long now, PooledConnection con, String username, String password) throws SQLException {
        PooledConnection pooledConnection = super.borrowConnection(now, con, username, password);
        if (con.getLastConnected() > now) {
            preHandleConnection(pooledConnection);
        }
        return pooledConnection;
    }

    @Override
    protected PooledConnection createConnection(long now, PooledConnection notUsed, String username, String password)
            throws SQLException {
        PooledConnection pooledConnection;
        try {
            pooledConnection = super.createConnection(now, notUsed, username, password);
        } catch (Throwable e) {
            throw e;
        }
        preHandleConnection(pooledConnection);
        return pooledConnection;
    }

    private void preHandleConnection(PooledConnection conn) {
        Connection connection = conn == null ? null : conn.getConnection();
        if (connection != null) {
            trySetSessionWaitTimeout(connection);
        }
    }

    @Override
    protected void returnConnection(PooledConnection con) {
        try {
            if (con.getConnection().isClosed() && !con.isDiscarded()) {
                con.setDiscarded(true);
                logger.warn("Connection marked discarded: {}", getName());
            }
        } catch (SQLException e) {
            logger.error("[returnConnection]" + this, e);
        }

        super.returnConnection(con);
    }

    protected long getConnectionTimeout() {
        return CONNECTION_TIMEOUT;
    }

    private void trySetSessionWaitTimeout(Connection conn) {
        try {
            setSessionWaitTimeout(conn, SESSION_WAIT_TIMEOUT);
            if (isIntegrityTest()) {
                setDefaultCollationForUtf8mb4(conn);
            }
        } catch (Exception e) {
            logger.error("set sessionWaitTimeout exception for {}", getName(), e);
        }
    }

    private void setDefaultCollationForUtf8mb4(Connection conn) {
        try (Statement statement = conn.createStatement()) {
            if (statement != null) {
                statement.execute("set session default_collation_for_utf8mb4=utf8mb4_general_ci;");
            } else {
                logger.error("set default_collation_for_utf8mb4 error for null statement");
            }
        } catch (Exception e) {
            logger.error("set default_collation_for_utf8mb4 exception for {}", getName(), e);
        }
    }

    private void setSessionWaitTimeout(Connection conn, int sessionWaitTimeout) throws SQLException {
        boolean autoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(true);
            try (Statement statement = conn.createStatement()) {
                if (statement != null) {
                    statement.setQueryTimeout(1);
                    statement.execute(String.format("set session wait_timeout = %d", sessionWaitTimeout));
                    try (ResultSet rs = statement.executeQuery("show session variables like 'wait_timeout'")) {
                        if (rs.next() && sessionWaitTimeout == rs.getInt(2)) {
                            logger.info("set sessionWaitTimeout to {}s succeeded: {}", sessionWaitTimeout, getName());
                        } else {
                            logger.warn("check sessionWaitTimeout failed for {}", getName());
                        }
                    } catch (Throwable t) {
                        logger.warn("check sessionWaitTimeout exception for cluster {}", getName(), t);
                    }
                } else {
                    logger.error("set sessionWaitTimeout error for null statement");
                }
            }
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }
}
