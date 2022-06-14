package com.ctrip.framework.drc.replicator.impl.inbound.driver;

import com.ctrip.framework.drc.core.driver.AbstractMySQLConnector;
import com.ctrip.framework.drc.core.driver.ConnectionObserver;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.framework.drc.core.driver.command.netty.codec.ChannelHandlerFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.NettyClientWithEndpoint;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.replicator.impl.inbound.ReplicatorChannelHandlerFactory;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.utils.ThreadUtils.getThreadName;

/**
 * Created by mingdongli
 * 2019/9/20 上午10:56.
 */
public class ReplicatorPooledConnector extends AbstractMySQLConnector implements MySQLConnector {

    public ReplicatorPooledConnector(Endpoint endpoint) {
        super(endpoint);
    }

    @Override
    protected ChannelHandlerFactory getChannelHandlerFactory() {
        return new ReplicatorChannelHandlerFactory();
    }

    @Override
    protected ExecutorService getExecutorService() {
        return ThreadUtils.newSingleThreadExecutor(getThreadName(THREAD_NAME_PREFIX, threadNamePostfix));
    }

    @Override
    protected void postProcessSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool) throws Exception {
        NettyClient nettyClient = simpleObjectPool.borrowObject();
        NettyClientWithEndpoint nettyClientWithEndpoint = nettyClient.channel().attr(NettyClientFactory.KEY_CLIENT).get();
        CountDownLatch countDownLatch = nettyClientWithEndpoint.getCountDownLatch();
        boolean handshake = countDownLatch.await(2, TimeUnit.SECONDS);
        if (!handshake) {
            logger.error("[AUTHENTICATION] failed for {} and close", endpoint);
            simpleObjectPool.returnObject(nettyClient);
            throw new RuntimeException("handshake error");
        }
        logger.info("Finish [AUTHENTICATION] for {}", endpoint);
        simpleObjectPool.returnObject(nettyClient);
        for (Observer observer : connectionListeners) {
            if (observer instanceof ConnectionObserver) {
                observer.update(simpleObjectPool, this);
            }
        }
    }

    @Override
    public String getModuleName() {
        return ModuleEnum.REPLICATOR.getDescription();
    }

    @Override
    public boolean autoRead() {
        return false;
    }
}
