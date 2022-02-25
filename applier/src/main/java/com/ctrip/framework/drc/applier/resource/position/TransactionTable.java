package com.ctrip.framework.drc.applier.resource.position;

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

    void recordToMemory(String gtid);

    void mergeRecord(String uuid, boolean needRetry);
}
