package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.exception.dump.NetworkException;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by mingdongli
 * 2019/9/23 下午5:01.
 */
public abstract class AbstractMySQLSlave extends AbstractLifecycle implements MySQLSlave {

    protected MySQLSlaveConfig mySQLSlaveConfig;

    protected MySQLConnection mySQLConnection;

    protected LogEventHandler eventHandler;

    protected ExecutorService parseExecutorService;

    protected Future parseFuture;

    public AbstractMySQLSlave(MySQLSlaveConfig mySQLSlaveConfig) {
        this.mySQLSlaveConfig = mySQLSlaveConfig;
        parseExecutorService = ThreadUtils.newSingleThreadExecutor(getClass().getSimpleName() + "-Binlog-Dump-" + mySQLSlaveConfig.getRegistryKey());
    }

    protected void doInitialize() throws Exception {
        super.doInitialize();
        mySQLConnection = buildConnection();
        LifecycleHelper.initializeIfPossible(mySQLConnection);
    }

    protected void doStart() throws Exception {
        super.doStart();
        LifecycleHelper.startIfPossible(mySQLConnection);
        mySQLConnection.preDump();
        parseFuture = parseExecutorService.submit(() -> {
            if (getLifecycleState().isInitialized()) {
                try {
                    mySQLConnection.dump(resultCode -> eventHandler.onLogEvent(null, null, new NetworkException(resultCode.getMessage())));
                } catch (Exception e) {
                    logger.error("dump error", e);
                }
            }
        });
        mySQLConnection.postDump();
    }

    private MySQLConnection buildConnection() {
        if (mySQLSlaveConfig.getSlaveId() <= 0) {
            long serverId = generateUniqueServerId();
            logger.info("[Generate] unique server id {}", serverId);
            mySQLSlaveConfig.setSlaveId(serverId);
        }
        return newConnection(mySQLSlaveConfig, eventHandler);
    }

    protected abstract MySQLConnection newConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler);

    protected final long generateUniqueServerId() {
        InetSocketAddress socketAddress = mySQLSlaveConfig.getEndpoint().getSocketAddress();
        byte[] addr = socketAddress.getAddress().getAddress();
        String registryKey = mySQLSlaveConfig.getRegistryKey();
        int salt = (registryKey != null) ? registryKey.hashCode() : 0;
        return ((0x7f & salt) << 24) + ((0xff & (int) addr[1]) << 16) // NL
                + ((0xff & (int) addr[2]) << 8) // NL
                + (0xff & (int) addr[3]);
    }

    @Override
    public void setLogEventHandler(LogEventHandler logEventHandler) {
        this.eventHandler = logEventHandler;
    }


    @Override
    protected void doStop() throws Exception{
        super.doStop();
        LifecycleHelper.stopIfPossible(mySQLConnection);
        boolean canceled = parseFuture.cancel(true);
        logger.info("[parseFuture] cancel {}", canceled);
        parseExecutorService.shutdown();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        LifecycleHelper.disposeIfPossible(mySQLConnection);
    }
}
