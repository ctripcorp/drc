package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DbTransactionTableGtidReader implements GtidReader {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SELECT_TX_TABLE_GTID_SET = "select `server_uuid`, `gtidset` from `drcmonitordb`.`tx_%s` where `id` = -1;";
    private static final String SELECT_TX_TABLE_SPECIFIC_GTID_SET = "select `gno`, `gtidset` from `drcmonitordb`.`tx_%s` where `server_uuid` = \"%s\";";


    // just for logging
    private Endpoint endpoint;
    private String dbName;

    public DbTransactionTableGtidReader(Endpoint endpoint, String dbName) {
        this.endpoint = endpoint;
        this.dbName = dbName;
    }

    public DbTransactionTableGtidReader(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public String getExecutedGtids(Connection connection) throws Exception {
        GtidSet mergedGtidSet = selectDbGtidSet(connection);
        return mergedGtidSet.toString();
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private GtidSet selectDbGtidSet(Connection connection) throws Exception {
        return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.db.transaction.table.gtidset.reader.merged", dbName + "-" + endpoint.getHost() + ":" + endpoint.getPort(), () -> {
            GtidSet dbGtidSet = new GtidSet("");
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(String.format(SELECT_TX_TABLE_GTID_SET, dbName))) {

                while (resultSet.next()) {
                    dbGtidSet = dbGtidSet.union(new GtidSet(resultSet.getString(2)));
                }
            } catch (Exception e) {
                logger.warn("execute select sql error, sql is: {}", SELECT_TX_TABLE_GTID_SET, e);
                if (!e.getMessage().contains("doesn't exist")) {
                    throw e;
                }
            }
            return dbGtidSet;
        });
    }


    /*
     * tx_xx table structure
     * | id 	| server_uuid                          	| gno       	| gtidset                                          	|
     * |----	|--------------------------------------	|-----------	|--------------------------------------------------	|
     * | -1 	| 806ba835-3cd0-11ef-be08-fa163e3b758b 	| -1        	| 806ba835-3cd0-11ef-be08-fa163e3b758b:2-162411202 	|
     * | 0  	| 806ba835-3cd0-11ef-be08-fa163e3b758b 	| 162300000 	| null                                             	|
     * | 1  	| 806ba835-3cd0-11ef-be08-fa163e3b758b 	| 162300001 	| null                                             	|
     */
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public GtidSet getGtidSetByUuid(Connection connection, String uuid) throws SQLException {
        GtidSet specificGtidSet = new GtidSet("");
        String sql = String.format(SELECT_TX_TABLE_SPECIFIC_GTID_SET, dbName, uuid);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            while (resultSet.next()) {
                long gno = resultSet.getLong(1);
                if (gno == -1L) {
                    String gtidSet = resultSet.getString(2);
                    specificGtidSet = specificGtidSet.union(new GtidSet(gtidSet));
                } else {
                    specificGtidSet.add(uuid, gno);
                }
            }
        }
        return specificGtidSet;
    }
}
