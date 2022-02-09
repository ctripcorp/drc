package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
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

    private static final String SELECT_TRANSACTION_TABLE_SPECIFIC_GTID = "select `gno` from `drcmonitordb`.`gtid_executed` where `id` > -1 and `server_uuid` = \"%s\";";

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
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public GtidSet getSpecificGtidSet(Connection connection, String uuid) throws SQLException {
        GtidSet specificGtidSet = new GtidSet("");
        String sql = String.format(SELECT_TRANSACTION_TABLE_SPECIFIC_GTID, uuid);
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    specificGtidSet.add(uuid + ":" + resultSet.getLong(1));
                }
            }
        }
        return specificGtidSet;
    }
}
