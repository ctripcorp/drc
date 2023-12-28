package com.ctrip.framework.drc.core.monitor.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by dengquanliang
 * 2023/12/28 11:25
 */
public class DefaultWriteResource implements WriteResource {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private Connection connection;

    private Statement statement;

    private ResultSet resultSet;

    public DefaultWriteResource(Connection connection, Statement statement, ResultSet resultSet) {
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
