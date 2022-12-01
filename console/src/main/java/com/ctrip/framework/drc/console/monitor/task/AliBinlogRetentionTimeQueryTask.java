package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @ClassName AliBinlogRetentionTimeQueryTask
 * @Author haodongPan
 * @Date 2022/12/1 18:15
 * @Version: $
 */
public class AliBinlogRetentionTimeQueryTask implements NamedCallable<Long> {

    private static final String RDS_BINLOG_RETENTION_HOURS = "select value from mysql.rds_configuration where name = \"binlog retention hours\";";
    private static final int RDS_BINLOG_RETENTION_HOURS_INDEX = 1;

    private static final Logger logger = LoggerFactory.getLogger(AwsBinlogRetentionTimeQueryTask.class);


    private WriteSqlOperatorWrapper sqlOperatorWrapper;

    public AliBinlogRetentionTimeQueryTask(WriteSqlOperatorWrapper sqlOperatorWrapper) {
        this.sqlOperatorWrapper = sqlOperatorWrapper;
    }

    @Override
    public Long call() throws Exception {
        GeneralSingleExecution execution = new GeneralSingleExecution(RDS_BINLOG_RETENTION_HOURS);
        try (ReadResource readResource = sqlOperatorWrapper.select(execution)) {
            if (readResource == null) {
                return null;
            }
            ResultSet rs = readResource.getResultSet();
            if (rs.next()) {
                return rs.getLong(RDS_BINLOG_RETENTION_HOURS_INDEX);
            }
            return null;
        } catch (SQLException e) {
            logger.warn("AliBinlogRetentionTimeQueryTask query error",e);
            throw e;
        }
    }


    @Override
    public Logger getLogger() {
        return logger;
    }
}