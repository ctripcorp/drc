package com.ctrip.framework.drc.console.monitor.delay.impl.operator;

import com.ctrip.framework.drc.console.enums.SqlResultEnum;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.execution.SingleExecution;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.ctrip.framework.drc.core.monitor.operator.WriteBaseSqlOperator;
import com.ctrip.framework.drc.core.monitor.operator.WriteSqlOperatorV2;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by dengquanliang
 * 2023/12/28 15:43
 */
public abstract class AbstractWriteSqlOperator extends WriteBaseSqlOperator implements WriteSqlOperatorV2 {

    public AbstractWriteSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
        super(endpoint, poolProperties);
    }

    @Override
    public StatementExecutorResult writeWithResult(Execution execution) throws SQLException {
        return doWriteWithResult(execution);
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

                connection = writeDataSource.getConnection();
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
