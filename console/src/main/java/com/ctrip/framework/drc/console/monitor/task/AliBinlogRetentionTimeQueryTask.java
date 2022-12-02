package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


/**
 * @ClassName AliBinlogRetentionTimeQueryTask
 * @Author haodongPan
 * @Date 2022/12/1 18:15
 * @Version: $
 */
public class AliBinlogRetentionTimeQueryTask implements NamedCallable<Long> {

    private static final String ALI_BINLOG_RETENTION_HOURS = "show global variables like 'binlog_expire_logs_seconds';";
    private static final int ALI_BINLOG_RETENTION_HOURS_INDEX = 2;

    private static final Logger logger = LoggerFactory.getLogger(AwsBinlogRetentionTimeQueryTask.class);
    
    private DataSource dataSource;
    
    public AliBinlogRetentionTimeQueryTask(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Long call() throws Exception {
        
        try(Connection connection = dataSource.getConnection()) {
            try(Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(ALI_BINLOG_RETENTION_HOURS)) {
                    if (resultSet != null & resultSet.next()) {
                        return resultSet.getLong(ALI_BINLOG_RETENTION_HOURS_INDEX) / 3600;
                    }
                }
            }
        }
        return -1L;
    }

    @Override
    public void afterException(Throwable t) {
        logger.warn("AwsBinlogRetentionTimeQueryTask query error",t);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}