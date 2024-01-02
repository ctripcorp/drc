package com.ctrip.framework.drc.console.monitor.delay.impl.operator;

import com.ctrip.framework.drc.console.enums.SqlResultEnum;
import com.ctrip.framework.drc.console.monitor.consistency.sql.operator.AbstractSqlOperator;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.execution.SingleExecution;
import com.ctrip.framework.drc.core.monitor.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-06
 */
public abstract class ReadWriteAbstractSqlOperator extends AbstractSqlOperator implements ReadWriteSqlOperator {

    public ReadWriteAbstractSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
        super(endpoint, poolProperties);
    }

    @Override
    public void write(Execution execution) throws SQLException {
        doWrite(execution);
    }

    @Override
    public StatementExecutorResult writeWithResult(Execution execution) throws SQLException {
        return doWriteWithResult(execution);
    }

    @Override
    public void insert(Execution execution) throws SQLException {
        doWrite(execution);
    }

    @Override
    public void update(Execution execution) throws SQLException {
        doWrite(execution);
    }

    @Override
    public void delete(Execution execution) throws SQLException {
        doWrite(execution);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private void doWrite(Execution execution) throws SQLException {
        if (execution instanceof SingleExecution) {
            SingleExecution singleExecution = (SingleExecution) execution;
            String sql = singleExecution.getStatement();
            Connection connection = null;
            Statement statement = null;
            try {
                connection = dataSource.getConnection();
                statement = connection.createStatement();
                statement.execute(sql);
                logger.debug("[{}] execute [{}]", getClass().getSimpleName(), sql);
            } finally {
                if (null != statement) {
                    try {
                        statement.close();
                    } catch (SQLException t) {
                        logger.error("Failed to close statement for sql({}): ", sql, t);
                    }
                }
                if (null != connection) {
                    try {
                        connection.close();
                    } catch (SQLException t) {
                        logger.error("Failed to close connection for sql({}): ", sql, t);
                    }
                }
            }
        }
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private StatementExecutorResult doWriteWithResult(Execution execution) throws SQLException {
        StatementExecutorResult result = new StatementExecutorResult();
        if (execution instanceof SingleExecution) {
            SingleExecution singleExecution = (SingleExecution) execution;
            String sql = singleExecution.getStatement();
            Connection connection = null;
            Statement statement = null;
            try {

                connection = dataSource.getConnection();
                statement = connection.createStatement();
                int num = statement.executeUpdate(sql);
                logger.debug("[{}] execute [{}]", getClass().getSimpleName(), sql);
                result.setResult(SqlResultEnum.SUCCESS.getCode());
                result.setMessage(num + " rows updated");
            } catch (Throwable e) {
                result.setResult(SqlResultEnum.FAIL.getCode());
                result.setMessage(e.getMessage());
            } finally {
                if (null != statement) {
                    try {
                        statement.close();
                    } catch (SQLException t) {
                        logger.error("Failed to close statement for sql({}): ", sql, t);
                    }
                }
                if (null != connection) {
                    try {
                        connection.close();
                    } catch (SQLException t) {
                        logger.error("Failed to close connection for sql({}): ", sql, t);
                    }
                }
            }
        }
        return result;
    }
}
