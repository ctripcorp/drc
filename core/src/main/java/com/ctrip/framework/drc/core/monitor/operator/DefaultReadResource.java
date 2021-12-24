package com.ctrip.framework.drc.core.monitor.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by mingdongli
 * 2019/12/15 上午9:50.
 */
public class DefaultReadResource implements ReadResource {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Connection connection;

    private Statement statement;

    private ResultSet resultSet;

    public DefaultReadResource(Connection connection, Statement statement, ResultSet resultSet) {
        this.connection = connection;
        this.statement = statement;
        this.resultSet = resultSet;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    @Override
    public void close(){
        closeQuietly(resultSet);
        closeQuietly(statement);
        closeQuietly(connection);
    }

    private void closeQuietly(AutoCloseable closeable) {
        if(closeable != null) {
            try {
                logger.debug("try close {}", closeable);
                closeable.close();
            } catch (Exception e) {
                logger.error("Fail close {}, ", closeable, e);
            }
        }
    }
}
