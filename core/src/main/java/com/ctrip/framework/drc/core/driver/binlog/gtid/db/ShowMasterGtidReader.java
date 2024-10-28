package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by jixinwang on 2021/9/15
 */
public class ShowMasterGtidReader implements GtidReader {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected static final String ALI_RDS = "/*FORCE_MASTER*/";

    private static final String EXECUTED_GTID = ALI_RDS + "SELECT @@GLOBAL.gtid_executed;";

    private static final String EXECUTED_GTID_OLD = ALI_RDS + "show global variables like \"gtid_executed\";";

    private static final int EXECUTED_GTID_INDEX = 1;

    @Override
    public String getExecutedGtids(Connection connection) throws Exception {
        return select(connection, EXECUTED_GTID, EXECUTED_GTID_INDEX);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private String select(Connection connection, String sql, int index) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (DynamicConfig.getInstance().getOldGtidSqlSwitch()) {
                try (ResultSet resultSet = statement.executeQuery(EXECUTED_GTID_OLD)) {
                    if (resultSet.next()) {
                        return resultSet.getString(2);
                    }
                }
            }
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                if (resultSet.next()) {
                    try (Statement statementOld = connection.createStatement();
                         ResultSet resultSetOld = statementOld.executeQuery(EXECUTED_GTID_OLD)) {
                        if (resultSetOld.next()) {
                            String oldResult = resultSetOld.getString(2);
                            String newResult = resultSet.getString(index);
                            if (newResult != null && !newResult.equals(oldResult)) {
                                EventMonitor.DEFAULT.logEvent("gtid.query.different", oldResult);
                                logger.warn("gtid query result different, newResult: {}. oldResult: {}", newResult, oldResult);
                            }
                        }
                    }
                    return resultSet.getString(index);
                }
            }
        } catch (SQLException e) {
            logger.warn("execute select sql error, sql is: {}", sql, e);
            throw e;
        }
        return StringUtils.EMPTY;
    }
}
