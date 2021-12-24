package com.ctrip.framework.drc.console.monitor.consistency.sql.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.execution.SingleExecution;
import com.ctrip.framework.drc.core.monitor.operator.BaseSqlOperator;
import com.ctrip.framework.drc.core.monitor.operator.DefaultReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by mingdongli
 * 2019/11/12 下午6:07.
 */
public abstract class AbstractSqlOperator extends BaseSqlOperator {

    public AbstractSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
        super(endpoint, poolProperties);
    }

    @Override
    public ReadResource select(Execution execution) throws SQLException {
        if (execution instanceof SingleExecution) {
            SingleExecution singleExecution = (SingleExecution) execution;
            String sql = singleExecution.getStatement();
            Connection connection = null;
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                connection = dataSource.getConnection();
                statement = connection.createStatement();
                resultSet = statement.executeQuery(sql);
            } catch (CommunicationsException e) {
                logger.warn("Fail execute {}", sql, e);
            }
            return new DefaultReadResource(connection, statement, resultSet);
        }
        return null;
    }
}
