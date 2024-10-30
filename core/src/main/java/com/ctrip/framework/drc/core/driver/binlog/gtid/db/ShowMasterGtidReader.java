package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
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


    private String select(Connection connection, String sql, int index) throws SQLException {
        try {
            String oldResult = selectResult(connection, EXECUTED_GTID_OLD, 2);
            if (DynamicConfig.getInstance().getOldGtidSqlSwitch()) {
                return oldResult;
            }
            String newResult = selectResult(connection, sql, index);
            this.compareGtid(oldResult, newResult);
            return newResult;
        } catch (SQLException e) {
            logger.warn("execute select sql error, sql is: {}", sql, e);
            throw e;
        }
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private String selectResult(Connection connection, String sql, int index) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return resultSet.getString(index);
            } else {
                return StringUtils.EMPTY;
            }
        }
    }

    public boolean compareGtid(String oldResult, String newResult) {
        try {
            GtidSet oldGtidSet = new GtidSet(oldResult);
            GtidSet newGtidSet = new GtidSet(newResult);
            if (!oldGtidSet.isContainedWithin(newGtidSet)) {
                EventMonitor.DEFAULT.logEvent("gtid.query.different", oldResult);
                logger.warn("gtid query result different, not contained, newResult: {}. oldResult: {}", newResult, oldResult);
                return false;
            } else if (!newGtidSet.getUUIDs().equals(oldGtidSet.getUUIDs())) {
                EventMonitor.DEFAULT.logEvent("gtid.query.different", oldResult);
                logger.warn("gtid query result different, uuid different, newResult: {}. oldResult: {}", newResult, oldResult);
                return false;
            }else if (newGtidSet.subtract(oldGtidSet).getGtidNum() >= 1000) {
                EventMonitor.DEFAULT.logEvent("gtid.query.different", oldResult);
                logger.warn("gtid query result different, getGtidNum >= 1000, newResult: {}. oldResult: {}", newResult, oldResult);
                return false;
            } else if (oldGtidSet.getUUIDs().size() == 0 || newGtidSet.getUUIDs().size() == 0) {
                EventMonitor.DEFAULT.logEvent("gtid.query.different", oldResult);
                logger.warn("gtid query result different, gtidset empty, newResult: {}. oldResult: {}", newResult, oldResult);
                return false;
            }
            return true;
        } catch (Exception e) {
            EventMonitor.DEFAULT.logEvent("gtid.query.different.exception", oldResult);
            logger.warn("gtid query result different, newResult: {}. oldResult: {}, exception cause: {}", newResult, oldResult, e.getCause());
            return false;
        }
    }

}
