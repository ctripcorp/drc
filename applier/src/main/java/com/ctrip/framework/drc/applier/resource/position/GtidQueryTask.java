package com.ctrip.framework.drc.applier.resource.position;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.TransactionTableGtidReader;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
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

    private Endpoint endpoint;

    public GtidQueryTask(String uuid, Endpoint endpoint) {
        this.uuid = uuid;
        this.endpoint = endpoint;
    }

    @Override
    public GtidSet call() throws SQLException {
        loggerTT.info("[TT] query gtid set in db start, uuid is: {}", uuid);
        DataSource dataSource = DataSourceManager.getInstance().getDataSource(endpoint);
        GtidSet gtidSet;
        try (Connection connection = dataSource.getConnection()) {
            TransactionTableGtidReader gtidReader = new TransactionTableGtidReader();
            gtidSet = gtidReader.getGtidSetByUuid(connection, uuid);
            loggerTT.info("[TT] query gtid set in db success: {}", gtidSet.toString());
        } catch (SQLException e) {
            loggerTT.error("[TT] query gtid set in db failed, uuid is: {}", uuid, e);
            throw e;
        }
        return gtidSet;
    }

    @Override
    public void afterException(Throwable t) {
        DataSourceManager.getInstance().clearDataSource(endpoint);
        loggerTT.error("[TT] query gtid set task failed, uuid is: {}", uuid, t);
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            loggerTT.error("[TT] sleep error when calling gtid query task, uuid is: {}", uuid, e);
        }
    }
}
