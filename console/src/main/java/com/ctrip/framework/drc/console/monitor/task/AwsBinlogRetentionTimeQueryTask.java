package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


/**
 * @ClassName AwsBinlogRetentionTimeQueryTask
 * @Author haodongPan
 * @Date 2022/12/1 18:07
 * @Version: $
 */
public class AwsBinlogRetentionTimeQueryTask implements NamedCallable<Long> {

    private static final String RDS_BINLOG_RETENTION_HOURS = "select value from mysql.rds_configuration where name = \"binlog retention hours\";";
    private static final int RDS_BINLOG_RETENTION_HOURS_INDEX = 1;
    
    private static final Logger logger = LoggerFactory.getLogger(AwsBinlogRetentionTimeQueryTask.class);
    
    private DataSource dataSource;
    
    public AwsBinlogRetentionTimeQueryTask(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Long call() throws Exception {
        try(Connection connection = dataSource.getConnection()) {
            try(Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(RDS_BINLOG_RETENTION_HOURS)) {
                    if (resultSet != null & resultSet.next()) {
                        return resultSet.getLong(RDS_BINLOG_RETENTION_HOURS_INDEX);
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
