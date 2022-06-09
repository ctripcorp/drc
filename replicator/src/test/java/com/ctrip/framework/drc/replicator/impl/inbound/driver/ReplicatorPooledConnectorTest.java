package com.ctrip.framework.drc.replicator.impl.inbound.driver;

import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.NettyClientWithEndpoint;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

import static com.ctrip.framework.drc.replicator.AllTests.*;

/**
 * Created by mingdongli
 * 2019/10/26 下午11:27.
 */
public class ReplicatorPooledConnectorTest extends MockTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ReplicatorPooledConnector replicatorPooledConnector;

    @Mock
    private SimpleObjectPool<NettyClient> simpleObjectPool;

    @Mock
    private NettyClient nettyClient;

    @Mock
    private Channel channel;

    @Mock
    private NettyClientWithEndpoint nettyClientWithEndpoint;

    @Mock
    private Attribute<NettyClientWithEndpoint> attributeKey;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        Endpoint endpoint = new DefaultEndPoint(SRC_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD);
        replicatorPooledConnector = new ReplicatorPooledConnector(endpoint);
        replicatorPooledConnector.initialize();
        replicatorPooledConnector.start();
    }

    @After
    public void tearDown() throws Exception {
        replicatorPooledConnector.stop();
        replicatorPooledConnector.dispose();
    }

    @Test
    public void testConnectionWithoutServer() throws InterruptedException {
        ListenableFuture<SimpleObjectPool<NettyClient>> listenableFuture = replicatorPooledConnector.getConnectPool();
        Futures.addCallback(listenableFuture, new FutureCallback<SimpleObjectPool<NettyClient>>() {
            @Override
            public void onSuccess(SimpleObjectPool<NettyClient> result) {
                logger.info("listenableFuture success");
            }

            @Override
            public void onFailure(Throwable t) {
                Assert.assertNotNull(t);
            }
        });
        Thread.sleep( 1000);
    }

    @Test(expected = RuntimeException.class)
    public void testPostProcessSimpleObjectPoolWithException() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        when(simpleObjectPool.borrowObject()).thenReturn(nettyClient);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.attr(NettyClientFactory.KEY_CLIENT)).thenReturn(attributeKey);
        when(attributeKey.get()).thenReturn(nettyClientWithEndpoint);
        when(nettyClientWithEndpoint.getCountDownLatch()).thenReturn(countDownLatch);
        replicatorPooledConnector.postProcessSimpleObjectPool(simpleObjectPool, true);
        verify(simpleObjectPool, times(1)).returnObject(nettyClient);
    }

    @Test
    public void testPostProcessSimpleObjectPool() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.countDown();
        when(simpleObjectPool.borrowObject()).thenReturn(nettyClient);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.attr(NettyClientFactory.KEY_CLIENT)).thenReturn(attributeKey);
        when(attributeKey.get()).thenReturn(nettyClientWithEndpoint);
        when(nettyClientWithEndpoint.getCountDownLatch()).thenReturn(countDownLatch);
        replicatorPooledConnector.postProcessSimpleObjectPool(simpleObjectPool, true);
        verify(simpleObjectPool, times(1)).returnObject(nettyClient);
    }

    @Test
    public void testConnectionWithServer() throws InterruptedException {
        ListenableFuture<SimpleObjectPool<NettyClient>> listenableFuture = replicatorPooledConnector.getConnectPool();
        Futures.addCallback(listenableFuture, new FutureCallback<SimpleObjectPool<NettyClient>>() {
            @Override
            public void onSuccess(SimpleObjectPool<NettyClient> result) {
                logger.info("listenableFuture success");
            }

            @Override
            public void onFailure(Throwable t) {
                Assert.assertNotNull(t);
            }
        });
        Thread.sleep( 10000);
    }

}