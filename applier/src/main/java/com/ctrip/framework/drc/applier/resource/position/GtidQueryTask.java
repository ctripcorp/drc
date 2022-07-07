package com.ctrip.framework.drc.applier.resource.position;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.TransactionTableGtidReader;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2022/2/25
 */
public class GtidQueryTask implements NamedCallable<GtidSet> {

    protected final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    private String uuid;

    private DataSource dataSource;

    private String registryKey;

    public GtidQueryTask(String uuid, DataSource dataSource, String registryKey) {
        this.uuid = uuid;
        this.dataSource = dataSource;
        this.registryKey = registryKey;
    }

    @Override
    public GtidSet call() throws SQLException {
        loggerTT.info("[TT][{}] query gtid set in db start, uuid is: {}", registryKey, uuid);
        GtidSet gtidSet;
        try (Connection connection = dataSource.getConnection()) {
            TransactionTableGtidReader gtidReader = new TransactionTableGtidReader();
            gtidSet = gtidReader.getGtidSetByUuid(connection, uuid);
            loggerTT.info("[TT][{}] query gtid set in db success: {}", registryKey, gtidSet.toString());
        } catch (SQLException e) {
            loggerTT.error("[TT][{}] query gtid set in db failed, uuid is: {}", registryKey, uuid, e);
            throw e;
        }
        return gtidSet;
    }

    @Override
    public void afterException(Throwable t) {
        loggerTT.error("[TT][{}] query gtid set task failed, uuid is: {}", registryKey, uuid, t);
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            loggerTT.error("[TT][{}] sleep error when calling gtid query task, uuid is: {}", registryKey, uuid, e);
        }
    }
}
