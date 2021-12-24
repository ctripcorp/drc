package com.ctrip.framework.drc.console.monitor.netty;

import com.ctrip.framework.drc.core.server.config.replicator.MySQLMasterConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-20
 */
public class NettyServerTest {

    private static final int PORT = 18383;

    private NettyServer nettyServer;

    @Before
    public void setUp() throws Exception {
        MySQLMasterConfig mySQLMasterConfig = new MySQLMasterConfig();
        mySQLMasterConfig.setPort(PORT);
        nettyServer = new NettyServer(mySQLMasterConfig);
        nettyServer.initialize();
    }

    @Test
    public void test() throws Exception {
        Assert.assertFalse(isUsed(PORT));
        nettyServer.start();
        Assert.assertTrue(isUsed(PORT));
    }

    private static boolean isUsed(int port) {
        // Creates a server socket, bound to the specified port. IOException will be thrown if the port is already occupied
        try (ServerSocket ignored = new ServerSocket(port)) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    @After
    public void tearDown() throws Exception {
        nettyServer.stop();
        nettyServer.dispose();
    }
}
