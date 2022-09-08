package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidConsumer;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by jixinwang on 2021/9/15
 */
public class TransactionTableGtidReader implements GtidReader {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SELECT_TRANSACTION_TABLE_GTID_SET = "select `server_uuid`, `gtidset` from `drcmonitordb`.`gtid_executed` where `id` = -1;";

    private static final String SELECT_TRANSACTION_TABLE_GTID = "select `server_uuid`, `gno` from `drcmonitordb`.`gtid_executed` where `id` > -1;";

    private static final String SELECT_TRANSACTION_TABLE_SPECIFIC_GTID_SET = "select `gno`, `gtidset` from `drcmonitordb`.`gtid_executed` where `server_uuid` = \"%s\";";

    // just for logging
    private Endpoint endpoint;

    public TransactionTableGtidReader(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public TransactionTableGtidReader() {
    }

    @Override
    public String getExecutedGtids(Connection connection) {
        GtidSet mergedGtidSet = selectMergedGtidSet(connection, SELECT_TRANSACTION_TABLE_GTID_SET);
        GtidSet unMergedGtidSet = selectUnMergedGtidSet(connection, SELECT_TRANSACTION_TABLE_GTID);
        return mergedGtidSet.union(unMergedGtidSet).toString();
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private GtidSet selectMergedGtidSet(Connection connection, String sql) {
        return DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("DRC.transaction.table.gtidset.reader.merged", endpoint.getHost() + ":" + endpoint.getPort(), () ->
        {
            GtidSet executedGtidSet = new GtidSet("");
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        executedGtidSet = executedGtidSet.union(new GtidSet(resultSet.getString(2)));
                    }
                }
            } catch (SQLException e) {
                logger.warn("execute select sql error, sql is: {}", sql, e);
            }
            return executedGtidSet;
        });
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private GtidSet selectUnMergedGtidSet(Connection connection, String sql) {
        return DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("DRC.transaction.table.gtidset.reader.unmerged", endpoint.getHost() + ":" + endpoint.getPort(), () ->
        {
            GtidConsumer gtidConsumer = new GtidConsumer(true);
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        gtidConsumer.offer(String.format("%s:%s", resultSet.getString(1), resultSet.getString(2)));
                    }
                }
            } catch (SQLException e) {
                logger.warn("execute select sql error, sql is: {}", sql, e);
            }
            return gtidConsumer.getGtidSet();
        });
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public GtidSet getGtidSetByUuid(Connection connection, String uuid) throws SQLException {
        GtidSet specificGtidSet = new GtidSet("");
        String sql = String.format(SELECT_TRANSACTION_TABLE_SPECIFIC_GTID_SET, uuid);
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    String gtidSet = resultSet.getString(2);
                    if (gtidSet != null) {
                        specificGtidSet = specificGtidSet.union(new GtidSet(gtidSet));
                    } else {
                        specificGtidSet.add(uuid + ":" + resultSet.getLong(1));
                    }
                }
            }
        }
        return specificGtidSet;
    }
}
