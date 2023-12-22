package com.ctrip.framework.drc.applier.resource.position;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;

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

    void merge(GtidSet gtidSet);

    void recordToMemory(String gtid);

    GtidSet mergeRecord(String uuid, boolean needRetry);
}
