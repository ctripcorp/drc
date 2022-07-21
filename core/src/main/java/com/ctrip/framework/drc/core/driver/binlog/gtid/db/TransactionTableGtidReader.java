package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

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
        GtidSet executedGtidSet = new GtidSet("");
        Map<String, String> gtidSet = select(connection, SELECT_TRANSACTION_TABLE_GTID_SET);
        for (Map.Entry<String, String> entry : gtidSet.entrySet()) {
            executedGtidSet = executedGtidSet.union(new GtidSet(entry.getValue()));
        }

        Map<String, String> gtids = select(connection, SELECT_TRANSACTION_TABLE_GTID);
        for (Map.Entry<String, String> entry : gtids.entrySet()) {
            executedGtidSet.add(entry.getKey() + ":" + entry.getValue());
        }

        return executedGtidSet.toString();
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private Map<String, String> select(Connection connection, String sql) {
        return DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("DRC.transaction.table.gtidset.reader", endpoint.getHost() + ":" + endpoint.getPort(), () ->
        {
            Map<String, String> result = Maps.newHashMap();
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        result.put(resultSet.getString(1), resultSet.getString(2));
                    }
                }
            } catch (SQLException e) {
                logger.warn("execute select sql error, sql is: {}", sql, e);
            }
            return result;
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
