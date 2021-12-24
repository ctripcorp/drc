package com.ctrip.framework.drc.console.resource;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

import javax.sql.DataSource;
import java.sql.*;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-22
 */
public class TransactionContextResource extends AbstractLifecycle {

    private static final String SHOW_MASTER_STATUS = "show master status;";
    private static final int GTID_EXECUTED_INDEX = 5;
    private static final String SET_GTID_NEXT = "set gtid_next = '%s';";
    private static final String BEGIN = "begin;";
    private static final String COMMIT = "commit;";

    protected DataSource dataSource;
    protected Connection connection;

    public TransactionContextResource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public String getExecutedGtid() {
        try (Statement statement = connection.createStatement()){
            try(ResultSet gtidExecutedResultSet = statement.executeQuery(SHOW_MASTER_STATUS)) {
                if (gtidExecutedResultSet.next()) {
                    return gtidExecutedResultSet.getString(GTID_EXECUTED_INDEX);
                }
            }
        } catch (Throwable e) {
            logger.error("transaction.getExecutedGtid() - execute: ", e);
        }
        return null;
    }

    public void setGtid(String gtid) {
        try (PreparedStatement statement = connection.prepareStatement(String.format(SET_GTID_NEXT, gtid));){
            statement.execute();
        } catch (Throwable e) {
            logger.error("transaction.setGtid() - execute: ", e);
        }
    }

    public void begin() {
        try (PreparedStatement statement = connection.prepareStatement(BEGIN)) {
            statement.execute();
        } catch (Throwable e) {
            logger.error("transaction.begin() - execute", e);
        }
    }

    public void commit() {
        try (PreparedStatement statement = connection.prepareStatement(COMMIT)){
            statement.execute();
        } catch (Throwable e) {
            logger.error("transaction.commit() - execute: ", e);
        }
    }

    @Override
    public void doInitialize() throws Exception {
        super.doInitialize();
        connection = dataSource.getConnection();
    }

    @Override
    public void doDispose() throws Exception {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("connection.close(): ", e);
        }
        super.doDispose();
    }
}
