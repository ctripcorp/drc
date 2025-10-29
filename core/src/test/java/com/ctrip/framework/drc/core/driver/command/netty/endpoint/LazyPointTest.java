package com.ctrip.framework.drc.core.driver.command.netty.endpoint;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * Created by dengquanliang
 * 2025/8/20 15:15
 */
public class LazyPointTest {

    @Test
    public void testLazyPoint() {
        LazyEndPoint endPoint1 = new LazyEndPoint("127.0.0.1", 3306);
        System.out.println(endPoint1.toString());
        InetSocketAddress socketAddress = endPoint1.getSocketAddress();
        System.out.println(socketAddress.toString());


        DefaultEndPoint endPoint2 = new DefaultEndPoint("127.0.0.1", 3306);
        System.out.println(endPoint2.toString());
        InetSocketAddress socketAddress2 = endPoint2.getSocketAddress();
        System.out.println(socketAddress2.toString());

        Assert.assertEquals(socketAddress, socketAddress2);
    }
}
