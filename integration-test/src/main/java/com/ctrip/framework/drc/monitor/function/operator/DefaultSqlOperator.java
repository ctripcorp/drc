package com.ctrip.framework.drc.monitor.function.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:30.
 */
public class DefaultSqlOperator extends AbstractSqlOperator implements ReadWriteSqlOperator {

    public DefaultSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
        super(endpoint, poolProperties);
    }

    @Override
    public boolean write(Execution execution) {
        return doWrite(execution);
    }

    @Override
    public boolean insert(Execution execution) {
        return doWrite(execution);
    }

    @Override
    public boolean update(Execution execution) {
        return doWrite(execution);
    }

    @Override
    public boolean delete(Execution execution) {
        return doWrite(execution);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private boolean doWrite(Execution execution) {
        Collection<String> statements = execution.getStatements();
        Statement statement = null;
        try (final Connection connection = dataSource.getConnection()) {
            statement = connection.createStatement();
            for (String sql : statements) {
                statement.execute(sql);
                logger.debug("[{}] execute [{}]", getClass().getSimpleName(), sql);
            }
            return true;
        } catch (SQLException e) {
            logger.error("write {} error", StringUtils.join(execution.getStatements()), e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
        return false;
    }

    @Override
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public int[] batchInsert(Execution execution) {
        Collection<String> statements = execution.getStatements();
        Statement statement = null;
        try (final Connection connection = dataSource.getConnection()) {
            statement = connection.createStatement();
            String variablesSet = "set global max_allowed_packet=67108864";
            statement.execute(variablesSet);
        } catch (SQLException e) {
            logger.error("set global max_allowed_packet=67108864 error", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("statement close error", e);
                }
            }
        }
        
        try (final Connection connection = dataSource.getConnection()) {
            statement = connection.createStatement();
            for (String sql : statements) {
                statement.addBatch(sql);
                logger.debug("[{}] execute [{}]", getClass().getSimpleName(), sql);
            }
            return statement.executeBatch();
        } catch (SQLException e) {
            logger.error("write {} error", StringUtils.join(execution.getStatements()), e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }

        return new int[]{-1};
    }

}
