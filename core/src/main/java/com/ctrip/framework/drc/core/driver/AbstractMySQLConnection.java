package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by mingdongli
 * 2019/9/23 下午5:11.
 */
public abstract class AbstractMySQLConnection extends AbstractLifecycle implements MySQLConnection {

    protected MySQLConnector connector;

    protected MySQLSlaveConfig mySQLSlaveConfig;

    protected LogEventHandler logEventHandler;

    protected ScheduledExecutorService scheduledExecutorService;

    public AbstractMySQLConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler, MySQLConnector connector) {
        this.mySQLSlaveConfig = mySQLSlaveConfig;
        this.connector = connector;
        this.logEventHandler = eventHandler;
        scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(getClass().getSimpleName());
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        LifecycleHelper.initializeIfPossible(connector);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        LifecycleHelper.startIfPossible(connector);
    }

    @Override
    protected void doStop() throws Exception{
        LifecycleHelper.stopIfPossible(connector);
    }

    @Override
    protected void doDispose() throws Exception{
        LifecycleHelper.disposeIfPossible(connector);
        scheduledExecutorService.shutdown();
    }

    @Override
    public void preDump() throws Exception{

    }

    @Override
    public void postDump() {

    }
}
