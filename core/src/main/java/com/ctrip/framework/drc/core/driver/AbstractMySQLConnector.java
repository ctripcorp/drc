package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.driver.command.netty.AsyncNettyClientWithEndpoint;
import com.ctrip.framework.drc.core.driver.command.netty.DrcNettyClientPool;
import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.framework.drc.core.driver.command.netty.codec.ChannelHandlerFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy.ProxyEnabled;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.server.utils.ThreadUtils.getThreadName;

/**
 * Created by mingdongli
 * 2019/9/23 下午5:19.
 */
public abstract class AbstractMySQLConnector extends AbstractLifecycle implements MySQLConnector {

    public static final String THREAD_NAME_PREFIX = "SimpleObjectPool-Connector";

    protected List<Observer> connectionListeners = Lists.newCopyOnWriteArrayList();

    protected Endpoint endpoint;

    protected String threadNamePostfix;

    private SimpleObjectPool<NettyClient> objectPool;

    private ListeningExecutorService executorService;

    public AbstractMySQLConnector(Endpoint endpoint) {
        this.endpoint = endpoint;
        this.threadNamePostfix = endpoint.getSocketAddress().toString().replaceAll("/", "");
        NettyClientFactory nettyClientFactory = getNettyClientFactory(getThreadName(getModuleName(), threadNamePostfix), autoRead());
        nettyClientFactory.setHandlerFactory(getChannelHandlerFactory());
        this.objectPool = new DrcNettyClientPool(endpoint, nettyClientFactory);
    }

    protected NettyClientFactory getNettyClientFactory(String threadPrefix, boolean autoRead) {
        return new NettyClientFactory(this.endpoint, threadPrefix, autoRead());
    }

    @Override
    protected void doInitialize() throws Exception {
        executorService = MoreExecutors.listeningDecorator(getExecutorService());
        LifecycleHelper.initializeIfPossible(objectPool);
    }

    @Override
    protected void doStart() throws Exception {
        LifecycleHelper.startIfPossible(objectPool);
    }


    @Override
    protected void doStop() throws Exception {
        LifecycleHelper.stopIfPossible(objectPool);
    }

    @Override
    protected void doDispose() throws Exception {
        LifecycleHelper.disposeIfPossible(objectPool);
        executorService.shutdownNow();
    }

    @Override
    public ListenableFuture<SimpleObjectPool<NettyClient>> getConnectPool() {
        ListenableFuture<SimpleObjectPool<NettyClient>> listenableFuture = executorService.submit(() -> {
            try {
                postProcessSimpleObjectPool(objectPool);
            } catch (Throwable t) {
                throw t;
            }
            return objectPool;
        });
        return listenableFuture;
    }

    protected abstract ChannelHandlerFactory getChannelHandlerFactory();

    protected ExecutorService getExecutorService() {
        return ThreadUtils.newCachedThreadPool(getThreadName(THREAD_NAME_PREFIX, threadNamePostfix));
    }

    protected void postProcessSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool) throws Exception {
        if (endpoint instanceof ProxyEnabled) {
            NettyClient nettyClient = simpleObjectPool.borrowObject();
            if (nettyClient instanceof AsyncNettyClientWithEndpoint) {
                AsyncNettyClientWithEndpoint asyncNettyClientWithEndpoint = (AsyncNettyClientWithEndpoint) nettyClient;
                ChannelFuture channelFuture = asyncNettyClientWithEndpoint.getFuture();
                channelFuture.addListener(connFuture -> {
                    try {
                        if (connFuture.isSuccess()) {
                            Channel channel = nettyClient.channel();
                            ProxyEnabled proxyEnabled = (ProxyEnabled) endpoint;
                            channel.writeAndFlush(proxyEnabled.getProxyProtocol().output());
                        }
                    }  finally {
                        simpleObjectPool.returnObject(nettyClient);
                    }
                });
            }
        }
    }

    @Override
    public void addObserver(Observer observer) {
        if (!connectionListeners.contains(observer)) {
            connectionListeners.add(observer);
        }
    }

    @Override
    public void removeObserver(Observer observer) {
        connectionListeners.remove(observer);
    }

    @Override
    public boolean autoRead() {
        return true;
    }
}
