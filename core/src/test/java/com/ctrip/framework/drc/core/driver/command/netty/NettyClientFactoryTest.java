package com.ctrip.framework.drc.core.driver.command.netty;

import com.ctrip.framework.drc.core.AllTests;
import com.ctrip.framework.drc.core.MockTest;
import com.ctrip.framework.drc.core.driver.command.netty.codec.ChannelHandlerFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.google.common.collect.Lists;
import org.apache.commons.pool2.PooledObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/6/18
 */
public class NettyClientFactoryTest extends MockTest {

    private NettyClientFactory nettyClientFactory;

    @Mock
    private ChannelHandlerFactory handlerFactory;

    private Endpoint endpoint = new DefaultEndPoint(AllTests.IP, AllTests.SRC_PORT, AllTests.MYSQL_USER, AllTests.MYSQL_PASSWORD);

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        nettyClientFactory = new NettyClientFactory(endpoint, getClass().getSimpleName(), true);
        nettyClientFactory.setHandlerFactory(handlerFactory);
        when(handlerFactory.createChannelHandlers()).thenReturn(Lists.newArrayList());
        nettyClientFactory.start();
    }

    @After
    public void tearDown() throws Exception {
        nettyClientFactory.stop();
    }

    @Test
    public void makeObject() throws Exception {
        PooledObject<NettyClient> pooledObject = nettyClientFactory.makeObject();
        boolean validate = nettyClientFactory.validateObject(pooledObject);
        if (isUsed(AllTests.SRC_PORT)) {
            Assert.assertTrue(validate);
        } else {
            Assert.assertFalse(validate);
        }
        nettyClientFactory.destroyObject(pooledObject);
    }
}