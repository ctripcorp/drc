package com.ctrip.framework.drc.console.monitor.delay.impl.driver;

import com.ctrip.framework.drc.core.driver.command.netty.codec.ChannelHandlerFactory;
import com.ctrip.framework.drc.fetcher.activity.replicator.handler.FetcherChannelHandlerFactory;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.console.AllTests.ciEndpoint;

public class DelayMonitorPooledConnectorTest {

    private DelayMonitorPooledConnector connector = new DelayMonitorPooledConnector(ciEndpoint);

    @Test
    public void testGetChannelHandlerFactory() {
        ChannelHandlerFactory channelHandlerFactory = connector.getChannelHandlerFactory();
        Assert.assertTrue(channelHandlerFactory instanceof FetcherChannelHandlerFactory);
    }
}
