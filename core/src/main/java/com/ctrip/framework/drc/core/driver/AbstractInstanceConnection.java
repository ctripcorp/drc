package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2020/6/28
 */
public abstract class AbstractInstanceConnection extends AbstractMySQLConnection {

    public AbstractInstanceConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler, MySQLConnector connector) {
        super(mySQLSlaveConfig, eventHandler, connector);
    }

    @Override
    public void dump(DumpCallBack callBack) {

        ListenableFuture<SimpleObjectPool<NettyClient>> listenableFuture = connector.getConnectPool();
        Futures.addCallback(listenableFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(SimpleObjectPool<NettyClient> result) {
                doWithSimpleObjectPool(result, callBack, null);
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("listenableFuture error", t);
                dump(callBack);
            }
        });

    }

    abstract protected void doWithSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnectionCode);

    protected void doReconnect(DumpCallBack callBack, RECONNECTION_CODE reconnection_code) {
        if (getLifecycleState().isStarted()) {
            ListenableFuture<SimpleObjectPool<NettyClient>> listenableFuture = connector.getConnectPool();
            Futures.addCallback(listenableFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(SimpleObjectPool<NettyClient> result) {
                    doWithSimpleObjectPool(result, callBack, reconnection_code);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("[Reconnect] listenableFuture error", t);
                    scheduleReconnect(callBack, reconnection_code);
                }
            });
        } else {
            logger.info("[Reconnect] return for state {}", getLifecycleState().getPhaseName());
        }
    }

    protected void scheduleReconnect(DumpCallBack callBack, RECONNECTION_CODE reconnection_code) {
        if (getLifecycleState().isStarted()) {
            logger.info("[Schedule] reconnect task for {}", mySQLSlaveConfig.getRegistryKey());
            scheduledExecutorService.schedule(() -> doReconnect(callBack, reconnection_code), 1, TimeUnit.SECONDS);
        }
    }

    protected enum RECONNECTION_CODE {

        MORE_GTID_ERROR("Slave has more GTIDs than the master has"),

        PURGED_GTID_REQUIRED("purged binary logs containing GTIDs that the slave requires");

        private String message;

        public String getMessage() {
            return message;
        }

        RECONNECTION_CODE(String message) {
            this.message = message;
        }
    }

}
