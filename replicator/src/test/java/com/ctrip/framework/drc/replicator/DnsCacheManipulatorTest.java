package com.ctrip.framework.drc.replicator;

import com.alibaba.dcm.DnsCacheManipulator;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.utils.DnsCacheUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.Assert;
import org.junit.Test;

import java.security.Security;

/**
 * Created by dengquanliang
 * 2025/10/21 15:39
 */
public class DnsCacheManipulatorTest {
    private static final String IP1 = "rm-gs533kkpx8j9dhll2.mysql.singapore.rds.aliyuncs.com";
    private static final String IP2 = "fratestpub.cocfwf8nq1sz.eu-central-1.rds.amazonaws.com";
    private static final int PORT = 55944;
    private static final String USER = "root";
    private static final String PASSWORD = "root";

    @Test
    public void testRemoveDnsCache() {

        Endpoint endpoint = new DefaultEndPoint(IP2, PORT, USER, PASSWORD);
        Endpoint endpoint1 = new DefaultEndPoint(IP2, PORT, USER, PASSWORD);
        Assert.assertEquals(endpoint1, endpoint);
        DnsCacheUtils.setDnsCache(IP2, 3306, "127.0.0.1");
        DnsCacheUtils.setDnsCache(IP1, 3306, "127.0.0.2");
        Endpoint endpoint4 = new DefaultEndPoint(IP1, PORT, USER, PASSWORD);


        Endpoint endpoint2 = new DefaultEndPoint(IP2, PORT, USER, PASSWORD);
        Assert.assertNotEquals(endpoint2, endpoint);
        Assert.assertEquals(endpoint2.getSocketAddress().toString(), IP2 + "/127.0.0.1:55944");

        DnsCacheManipulator.removeDnsCache(IP2);
        Endpoint endpoint3 = new DefaultEndPoint(IP2, PORT, USER, PASSWORD);
        Assert.assertEquals(endpoint3, endpoint);

        Endpoint endpoint5 = new DefaultEndPoint(IP1, PORT, USER, PASSWORD);
        Assert.assertEquals(endpoint4, endpoint5);

        DnsCacheManipulator.removeDnsCache(IP1);
        Endpoint endpoint6 = new DefaultEndPoint(IP1, PORT, USER, PASSWORD);
        Assert.assertNotEquals(endpoint4, endpoint6);

    }

    @Test
    public void testDnsCacheExpire() throws InterruptedException {
        Endpoint endpoint = new DefaultEndPoint(IP2, PORT, USER, PASSWORD);
        DnsCacheManipulator.setDnsCache(500, IP2, "127.0.0.1");

        Endpoint endpoint1 = new DefaultEndPoint(IP2, PORT, USER, PASSWORD);
        Endpoint endpoint2 = new DefaultEndPoint(IP2, PORT, USER, PASSWORD);

        Thread.sleep(1000);

        Endpoint endpoint3 = new DefaultEndPoint(IP2, PORT, USER, PASSWORD);
        Assert.assertEquals(endpoint1, endpoint2);
        Assert.assertNotEquals(endpoint, endpoint1);
        Assert.assertEquals(endpoint3, endpoint);
    }
}