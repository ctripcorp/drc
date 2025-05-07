package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;

/**
 * @Author limingdong
 * @create 2022/4/7
 */
public class TableCreateTask extends BatchTask {

    public static final String FOREIGN_KEY_CHECKS = "SET FOREIGN_KEY_CHECKS=0";

    public TableCreateTask(List<String> sqls, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        super(inMemoryEndpoint, inMemoryDataSource);
        this.sqls.addAll(sqls);
    }

    @Override
    protected void executeBatch(Statement statement) throws SQLException {
        try {
            super.executeBatch(statement);
        } catch (SQLException e) {
            if (DynamicConfig.getInstance().getSkipUnsupportedTableSwitch()) {
                DDL_LOGGER.warn("[TableCreateTask] [problemSchemaSkip] {} ", statement, e);
            } else {
                DDL_LOGGER.error("[TableCreateTask] [problemSchema] {} ", statement, e);
                throw e;
            }
        }
    }

    @Override
    protected boolean beforeExecute(Statement statement) throws SQLException {
        boolean pass = statement.execute(FOREIGN_KEY_CHECKS);
        DDL_LOGGER.info("[Execute] {} with result {}", FOREIGN_KEY_CHECKS, pass);
        return pass;
    }
}
