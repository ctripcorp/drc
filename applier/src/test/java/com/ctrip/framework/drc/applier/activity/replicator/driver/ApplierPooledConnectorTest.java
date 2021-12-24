package com.ctrip.framework.drc.applier.activity.replicator.driver;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy.DrcProxyProtocol;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.applier.AllTests.HTTP_IP;

/**
 * @Author limingdong
 * @create 2021/4/13
 */
public class ApplierPooledConnectorTest {

    private ApplierPooledConnector applierPooledConnector;

    private Endpoint endpoint = new ProxyEnabledEndpoint(HTTP_IP, 8080, new DrcProxyProtocol("PROXY"));

    @Before
    public void setUp() throws Exception {
        applierPooledConnector = new ApplierPooledConnectorMock(endpoint);
        LifecycleHelper.initializeIfPossible(applierPooledConnector);
        LifecycleHelper.startIfPossible(applierPooledConnector);
    }

    @Test
    public void testSendProtocol() throws Exception {
        ListenableFuture<SimpleObjectPool<NettyClient>> listenableFuture = applierPooledConnector.getConnectPool();
        SimpleObjectPool<NettyClient> simpleObjectPool = listenableFuture.get(1000, TimeUnit.MILLISECONDS);
        NettyClient nettyClient = simpleObjectPool.borrowObject();
        Assert.assertNotNull(nettyClient);
        simpleObjectPool.returnObject(nettyClient);
    }

    @After
    public void tearDown() throws Exception {
        LifecycleHelper.stopIfPossible(applierPooledConnector);
        LifecycleHelper.disposeIfPossible(applierPooledConnector);
    }

    class ApplierPooledConnectorMock extends ApplierPooledConnector {

        public ApplierPooledConnectorMock(Endpoint endpoint) {
            super(endpoint);
        }

        @Override
        public boolean autoRead() {
            return false;
        }
    }
}