package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @ClassName BtdhsQueryTask
 * @Author haodongPan
 * @Date 2022/12/1 18:16
 * @Version: $
 */
public class BtdhsQueryTask implements NamedCallable<Long> {

    private static final String BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE = "show global variables like \"binlog_transaction_dependency_history_size\";";
    private static final int BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_INDEX = 2;

    private static final Logger logger = LoggerFactory.getLogger(BtdhsQueryTask.class);
    
    private DataSource dataSource;

    public BtdhsQueryTask(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Long call() throws Exception {

        try(Connection connection = dataSource.getConnection()) {
            try(Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE)) {
                    if (resultSet != null & resultSet.next()) {
                        return resultSet.getLong(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_INDEX);
                    }
                }
            }
        }
        return -1L;
    }

    @Override
    public void afterException(Throwable t) {
        logger.warn("BtdhsQueryTask query error",t);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}
