package com.ctrip.framework.drc.console.monitor.consistency.sql.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.execution.SingleExecution;
import com.ctrip.framework.drc.core.monitor.operator.DefaultReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class StreamQuerySqlOperator extends AbstractSqlOperator {

    private int fetchSize = 0;

    private int queryTimeOut = 0;

    public StreamQuerySqlOperator(Endpoint endpoint, PoolProperties poolProperties, int fetchSize, int queryTimeOut) {
        super(endpoint, poolProperties);
        this.fetchSize = fetchSize;
        this.queryTimeOut = queryTimeOut;
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
                // org.apache.tomcat.jdbc.pool.PoolExhaustedException: [ConsistencyCheckContainer-Work-256] Timeout: Pool empty. Unable to fetch a connection in 10 seconds, none available[size:100; busy:100; idle:0; lastwait:10000].
                //	at org.apache.tomcat.jdbc.pool.ConnectionPool.borrowConnection(ConnectionPool.java:686) ~[tomcat-jdbc-7.0.73.jar:?]
                //	at org.apache.tomcat.jdbc.pool.ConnectionPool.getConnection(ConnectionPool.java:189) ~[tomcat-jdbc-7.0.73.jar:?]
                //	at org.apache.tomcat.jdbc.pool.DataSourceProxy.getConnection(DataSourceProxy.java:128) ~[tomcat-jdbc-7.0.73.jar:?]
                //	at com.ctrip.framework.drc.console.monitor.consistency.sql.operator.StreamQuerySqlOperator.select(StreamQuerySqlOperator.java:40) ~[classes/:?]
                connection = dataSource.getConnection();
                statement = connection.createStatement();
                statement.setFetchSize(fetchSize);
                statement.setQueryTimeout(queryTimeOut);
                resultSet = statement.executeQuery(sql);
            } catch (CommunicationsException e) {
                logger.warn("Fail execute {}", sql, e);
            }
            return new DefaultReadResource(connection, statement, resultSet);
        }
        return null;
    }

    public String getIPAndPort() {
        return endpoint.getHost() + ":" + endpoint.getPort();
    }
}
