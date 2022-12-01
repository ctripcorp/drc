package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName BtdhsQueryTask
 * @Author haodongPan
 * @Date 2022/12/1 18:16
 * @Version: $
 */
public class BtdhsQueryTask implements NamedCallable<Long> {

    private static final String BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE = "show global variables like \"binlog_transaction_dependency_history_size\";";
    private static final int BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_INDEX = 2;

    private static final Logger logger = LoggerFactory.getLogger(AwsBinlogRetentionTimeQueryTask.class);


    private WriteSqlOperatorWrapper sqlOperatorWrapper;

    public BtdhsQueryTask(WriteSqlOperatorWrapper sqlOperatorWrapper) {
        this.sqlOperatorWrapper = sqlOperatorWrapper;
    }

    @Override
    public Long call() throws Exception {
        GeneralSingleExecution execution = new GeneralSingleExecution(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE);
        ReadResource readResource = null;
        try {
            readResource = sqlOperatorWrapper.select(execution);
            if (readResource == null) {
                return -1L;
            }
            ResultSet rs = readResource.getResultSet();
            if (rs.next()) {
                return rs.getLong(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_INDEX);
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
        logger.warn("BtdhsQueryTask query error",t);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}
