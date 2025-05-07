package com.ctrip.framework.drc.fetcher.resource.position;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Created by dengquanliang
 * 2025/1/10 16:04
 */
public class MessengerGtidMergeTask implements NamedCallable<Boolean> {

    protected final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    private static final String BEGIN = "begin";

    private static final String COMMIT = "commit";

    private static final String SELECT_GTID_SET_SQL = "select `gtid_set` from `drcmonitordb`.`messenger_gtid_executed` where `registry_key` = ? for update;";

    private static final String UPDATE_GTID_SET_SQL = "update `drcmonitordb`.`messenger_gtid_executed` set `gtid_set` = ? where `registry_key` = ?;";

    private static final String INSERT_GTID_SET_SQL = "insert into `drcmonitordb`.`messenger_gtid_executed`(`registry_key`, `gtid_set`) values(?, ?);";

    private String gtidExecuted;

    private DataSource dataSource;

    private String registryKey;

    public MessengerGtidMergeTask(String gtidExecuted, DataSource dataSource, String registryKey) {
        this.gtidExecuted = gtidExecuted;
        this.dataSource = dataSource;
        this.registryKey = registryKey;
    }

    @Override
    public Boolean call() throws Exception {
        return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.messenger.gtidset.write", registryKey, this::updateGtidSetRecord);
    }

    @Override
    public void afterException(Throwable t) {
        loggerTT.error("[TT][{}] call gtid merge task failed", registryKey, t);
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            loggerTT.error("[TT][{}] sleep error when calling gtid merge task", registryKey, e);
        }
    }

    @Override
    public void afterSuccess(int retryTime) {
        loggerTT.info("[TT][{}] {} success with retryTime {}", registryKey, name(), retryTime);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private boolean updateGtidSetRecord() throws SQLException {
        loggerTT.info("[TT][{}] use the gtid set: {} to union the old gtid set record", registryKey, gtidExecuted);
        try (Connection connection = dataSource.getConnection()){
            try (PreparedStatement statement = connection.prepareStatement(BEGIN)) {
                statement.execute();
            }
            String gtidSetFromDb = null;
            try (PreparedStatement statement = connection.prepareStatement(SELECT_GTID_SET_SQL)) {
                statement.setString(1, registryKey);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        gtidSetFromDb = result.getString("gtid_set");
                    }
                }
            }
            if (gtidSetFromDb == null) {
                loggerTT.info("[TT][{}] use the gtid set: {} to insert", registryKey, gtidExecuted);
                try (PreparedStatement insertStatement = connection.prepareStatement(INSERT_GTID_SET_SQL)) {
                    insertStatement.setString(1, registryKey);
                    insertStatement.setString(2, gtidExecuted);
                    if (insertStatement.executeUpdate() != 1) {
                        throw new SQLException("[TT] insert gtid set error, affected rows not 1");
                    }
                }
            } else {
                loggerTT.info("[TT][{}] use the new gtid set: {} to update the old gtid set record: {}", registryKey, gtidExecuted, gtidSetFromDb);
                try (PreparedStatement statement = connection.prepareStatement(UPDATE_GTID_SET_SQL)) {
                    statement.setString(1, gtidExecuted);
                    statement.setString(2, registryKey);
                    if (statement.executeUpdate() != 1) {
                        throw new SQLException("[TT][{}] update gtid set error, affected rows not 1", registryKey);
                    }
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(COMMIT)) {
                statement.execute();
            }
        } catch (SQLException e) {
            loggerTT.error("[TT][{}] update gtid set of {} error and clear from dataSourceManager", registryKey, dataSource.getUrl(), e);
            throw e;
        }
        return true;
    }
}
