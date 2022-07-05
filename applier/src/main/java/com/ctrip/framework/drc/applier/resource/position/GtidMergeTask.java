package com.ctrip.framework.drc.applier.resource.position;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2022/2/25
 */
public class GtidMergeTask implements NamedCallable<Boolean> {

    protected final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    private static final String BEGIN = "begin";

    private static final String COMMIT = "commit";

    private static final String SELECT_GTID_SET_SQL = "select `gtidset` from `drcmonitordb`.`gtid_executed` where `id` = -1 and `server_uuid` = ? for update;";

    private static final String UPDATE_GTID_SET_SQL = "update `drcmonitordb`.`gtid_executed` set `gtidset` = ? where `id` = -1 and `server_uuid` = ?;";

    private static final String INSERT_GTID_SET_SQL = "insert into `drcmonitordb`.`gtid_executed`(`id`, `server_uuid`, `gno`, `gtidset`) values(-1, ?, -1, ?);";

    private GtidSet gtidSet;

    private DataSource dataSource;

    public GtidMergeTask(GtidSet gtidSet, DataSource dataSource) {
        this.gtidSet = gtidSet;
        this.dataSource = dataSource;
    }

    @Override
    public Boolean call() throws SQLException {
        return updateGtidSetRecord(gtidSet);
    }

    @Override
    public void afterException(Throwable t) {
        loggerTT.error("[TT] call gtid merge task failed", t);
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            loggerTT.error("[TT] sleep error when calling gtid merge task", e);
        }
    }

    @Override
    public void afterSuccess(int retryTime) {
        loggerTT.info("{} success with retryTime {}", name(), retryTime);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private boolean updateGtidSetRecord(GtidSet gtidSet) throws SQLException {
        loggerTT.info("[TT] use the gtid set: {} to union the old gtid set record", gtidSet.toString());
        try (Connection connection = dataSource.getConnection()){
            try (PreparedStatement statement = connection.prepareStatement(BEGIN)) {
                statement.execute();
            }
            for (String uuid : gtidSet.getUUIDs()) {
                String gtidSetFromDb = null;
                try (PreparedStatement statement = connection.prepareStatement(SELECT_GTID_SET_SQL)) {
                    statement.setString(1, uuid);
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            gtidSetFromDb = result.getString("gtidset");
                        }
                    }
                }
                if (gtidSetFromDb == null) {
                    String gtidSetToInsert = gtidSet.getUUIDSet(uuid).toString();
                    loggerTT.info("[TT] use the gtid set: {} to insert", gtidSetToInsert);
                    try (PreparedStatement insertStatement = connection.prepareStatement(INSERT_GTID_SET_SQL)) {
                        insertStatement.setString(1, uuid);
                        insertStatement.setString(2, gtidSetToInsert);
                        if (insertStatement.executeUpdate() != 1) {
                            throw new SQLException("[TT] insert gtid set error, affected rows not 1");
                        }
                    }
                } else {
                    String gtidSetToUpdate = new GtidSet(gtidSetFromDb).union(gtidSet).getUUIDSet(uuid).toString();
                    loggerTT.info("[TT] use the new gtid set: {} to update the old gtid set record: {}", gtidSetToUpdate, gtidSetFromDb);
                    try (PreparedStatement statement = connection.prepareStatement(UPDATE_GTID_SET_SQL)) {
                        statement.setString(1, gtidSetToUpdate);
                        statement.setString(2, uuid);
                        if (statement.executeUpdate() != 1) {
                            throw new SQLException("[TT] update gtid set error, affected rows not 1");
                        }
                    }
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(COMMIT)) {
                statement.execute();
            }
        } catch (SQLException e) {
            loggerTT.error("update gtid set of {} error and clear from dataSourceManager", dataSource.getUrl(), e);
            throw e;
        }
        return true;
    }
}
