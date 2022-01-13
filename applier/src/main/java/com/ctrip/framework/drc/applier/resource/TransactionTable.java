package com.ctrip.framework.drc.applier.resource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by jixinwang on 2021/8/25
 */
public interface TransactionTable {

    void begin(String gtid) throws InterruptedException;

    void record(Connection connection, String gtid) throws SQLException;

    void rollback(String gtid);

    void commit(String gtid);

    void recordOppositeGtid(String gtid);

    void mergeOppositeGtid(boolean needRetry);

    void mergeRecordsFromDB() throws SQLException;
}
