package com.ctrip.framework.drc.console.monitor.delay.server;

import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.monitor.delay.impl.driver.DelayMonitorPooledConnector;
import com.ctrip.framework.drc.console.monitor.delay.task.PeriodicalUpdateDbTask;
import com.ctrip.framework.drc.console.monitor.netty.NettyServer;
import com.ctrip.framework.drc.console.utils.HeartbeatRequestHandler;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.command.netty.codec.ChannelHandlerFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.server.config.replicator.MySQLMasterConfig;
import com.ctrip.framework.drc.fetcher.activity.replicator.handler.FetcherChannelHandlerFactory;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import io.netty.channel.ChannelHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-30
 */
public class StaticDelayMonitorServerTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String IP = "127.0.0.1";

    private static final int PORT = 18383;

    private Endpoint endpoint = new DefaultEndPoint(IP, PORT, "", "");

    private NettyServer nettyServer;

    private StaticDelayMonitorServer staticDelayMonitorClient;

    private MySQLMasterConfig mySQLMasterConfig;

    private DelayMonitorSlaveConfig config = new DelayMonitorSlaveConfig();

    private MySQLConnector connector;

    private HeartbeatRequestHandler heartbeatRequestHandler = HeartbeatRequestHandler.getInstance();

    @Before
    public void setUp() throws Exception {
        /**
         * set up the netty server: mock replicator for two cluster
         */
        mySQLMasterConfig = new MySQLMasterConfig();
        mySQLMasterConfig.setPort(PORT);
        nettyServer = new NettyServer(mySQLMasterConfig);
        nettyServer.initialize();

        /**
         * set up the delay monitor server which includes netty client
         */
        connector = new TestDelayMonitorPooledConnector(endpoint);
        config.setDc("ut_dc");
        config.setDestDc("ut_destDc");
        config.setCluster("ut_cluster");
        config.setMha("ut_mha");
        config.setDestMha("ut_destMha");
        config.setRegistryKey("ut_" + getClass().getSimpleName());
        config.setEndpoint(endpoint);
        config.setMeasurement("ut_measurement");

        staticDelayMonitorClient = new StaticDelayMonitorServer(config, connector, new PeriodicalUpdateDbTask(), 864500000L);
        staticDelayMonitorClient.initialize();
    }

    @Test
    public void testReconnectionAfterReplicatorRestart() throws Exception {
        nettyServer.start();
        staticDelayMonitorClient.start();
        logger.info("Sleep 2s...");
        Thread.sleep(2000);
        Assert.assertTrue(System.currentTimeMillis() - heartbeatRequestHandler.testLastReadTime < 2000);
        nettyServer.stop();
        nettyServer.dispose();
        logger.info("Sleep 3s...");
        Thread.sleep(3000);
        Assert.assertTrue(System.currentTimeMillis() - heartbeatRequestHandler.testLastReadTime > 3000);
        nettyServer = new NettyServer(mySQLMasterConfig);
        nettyServer.initialize();
        nettyServer.start();
        logger.info("Sleep 2s...");
        Thread.sleep(2000);
        Assert.assertTrue(System.currentTimeMillis() - heartbeatRequestHandler.testLastReadTime < 2000);
    }

    @After
    public void tearDown() throws Exception {
        staticDelayMonitorClient.stop();
        nettyServer.stop();
        nettyServer.dispose();
    }

    class TestDelayMonitorPooledConnector extends DelayMonitorPooledConnector {

        public TestDelayMonitorPooledConnector(Endpoint endpoint) {
            super(endpoint);
        }

        @Override
        protected ChannelHandlerFactory getChannelHandlerFactory() {
            return new TestDelayMonitorChannelHandlerFactory();
        }
    }

    class TestDelayMonitorChannelHandlerFactory extends FetcherChannelHandlerFactory {
        @Override
        public List<ChannelHandler> createChannelHandlers() {
            List<ChannelHandler> handlers = super.createChannelHandlers();
            handlers.add(0, heartbeatRequestHandler);
            return handlers;
        }
    }
}
