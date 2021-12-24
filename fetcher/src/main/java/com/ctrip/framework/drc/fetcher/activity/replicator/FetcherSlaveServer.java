package com.ctrip.framework.drc.fetcher.activity.replicator;

import com.ctrip.framework.drc.core.driver.AbstractMySQLSlave;
import com.ctrip.framework.drc.core.driver.MySQLConnection;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.MySQLSlave;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.fetcher.activity.replicator.driver.FetcherConnection;
import com.ctrip.framework.drc.fetcher.resource.context.NetworkContextResource;

import java.util.concurrent.TimeUnit;

/**
 * Created by mingdongli
 * 2019/9/23 下午4:42.
 */
public class FetcherSlaveServer extends AbstractMySQLSlave implements MySQLSlave {

    private MySQLConnector mySQLConnector;

    private NetworkContextResource networkContextResource;

    private ByteBufConverter byteBufConverter;

    public FetcherSlaveServer(MySQLSlaveConfig mySQLSlaveConfig, MySQLConnector mySQLConnector, ByteBufConverter byteBufConverter) {
        super(mySQLSlaveConfig);
        this.mySQLConnector = mySQLConnector;
        this.byteBufConverter = byteBufConverter;
    }

    @Override
    protected MySQLConnection newConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler) {
        return new FetcherConnection(mySQLSlaveConfig, eventHandler, mySQLConnector, networkContextResource, byteBufConverter);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        if (!parseExecutorService.awaitTermination(1, TimeUnit.SECONDS)) {
            parseExecutorService.shutdownNow();
        }
    }

    public void setNetworkContextResource(NetworkContextResource networkContextResource) {
        this.networkContextResource = networkContextResource;
    }
}
