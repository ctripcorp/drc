package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;


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


    private WriteSqlOperatorWrapper sqlOperatorWrapper;

    public AliBinlogRetentionTimeQueryTask(WriteSqlOperatorWrapper sqlOperatorWrapper) {
        this.sqlOperatorWrapper = sqlOperatorWrapper;
    }

    @Override
    public Long call() throws Exception {
        GeneralSingleExecution execution = new GeneralSingleExecution(ALI_BINLOG_RETENTION_HOURS);
        ReadResource readResource = null;
        try {
            readResource = sqlOperatorWrapper.select(execution);
            if (readResource == null) {
                return -1L;
            }
            ResultSet rs = readResource.getResultSet();
            if (rs != null & rs.next()) {
                return rs.getLong(ALI_BINLOG_RETENTION_HOURS_INDEX);
            }
        } finally {
            if (readResource != null) {
                readResource.close();
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