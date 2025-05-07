package com.ctrip.framework.drc.messenger.activity.replicator.driver;

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

import static com.ctrip.framework.drc.messenger.AllTests.HTTP_IP;
import static org.junit.Assert.*;

/**
 * Created by shiruixin
 * 2024/11/11 14:17
 */
public class MqPooledConnectorTest {
    private MqPooledConnector mqPooledConnector;

    private Endpoint endpoint = new ProxyEnabledEndpoint(HTTP_IP, 8080, new DrcProxyProtocol("PROXY"));

    @Before
    public void setUp() throws Exception {
        mqPooledConnector = new MqPooledConnectorMock(endpoint);
        LifecycleHelper.initializeIfPossible(mqPooledConnector);
        LifecycleHelper.startIfPossible(mqPooledConnector);
    }

    @Test
    public void testSendProtocol() throws Exception {
        ListenableFuture<SimpleObjectPool<NettyClient>> listenableFuture = mqPooledConnector.getConnectPool();
        SimpleObjectPool<NettyClient> simpleObjectPool = listenableFuture.get(1000, TimeUnit.MILLISECONDS);
        NettyClient nettyClient = simpleObjectPool.borrowObject();
        Assert.assertNotNull(nettyClient);
        simpleObjectPool.returnObject(nettyClient);
    }

    @After
    public void tearDown() throws Exception {
        LifecycleHelper.stopIfPossible(mqPooledConnector);
        LifecycleHelper.disposeIfPossible(mqPooledConnector);
    }

    class MqPooledConnectorMock extends MqPooledConnector {

        public MqPooledConnectorMock(Endpoint endpoint) {
            super(endpoint);
        }

        @Override
        public boolean autoRead() {
            return false;
        }
    }

}