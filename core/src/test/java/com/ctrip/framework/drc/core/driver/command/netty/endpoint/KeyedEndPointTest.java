package com.ctrip.framework.drc.core.driver.command.netty.endpoint;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author yongnian
 * @create 2025/1/6 18:14
 */
public class KeyedEndPointTest {
    @Test
    public void testEquals() {
        DefaultEndPoint endPoint11 = new DefaultEndPoint("10.10.10.10", 8080, "test", "test");
        DefaultEndPoint endPoint12 = new DefaultEndPoint("10.10.10.10", 8080, "test", "test");
        DefaultEndPoint endPoint2 = new DefaultEndPoint("10.10.10.20", 8080, "test", "test");
        Assert.assertEquals(endPoint11, endPoint12);
        Assert.assertNotEquals(endPoint11, endPoint2);

        String registryKey = "registryKey";
        // same endpoint, same registryKey
        Assert.assertEquals(new KeyedEndPoint(registryKey, endPoint11), new KeyedEndPoint(registryKey, endPoint11));
        Assert.assertEquals(new KeyedEndPoint(registryKey, endPoint11), new KeyedEndPoint(registryKey, endPoint12));

        // same endpoint, different registryKey
        Assert.assertNotEquals(new KeyedEndPoint(registryKey, endPoint11), new KeyedEndPoint(registryKey + "_different", endPoint11));
        Assert.assertNotEquals(new KeyedEndPoint(registryKey, endPoint11), new KeyedEndPoint(registryKey + "_different", endPoint12));

        // different endpoint, same registryKey
        Assert.assertNotEquals(new KeyedEndPoint(registryKey, endPoint11), new KeyedEndPoint(registryKey, endPoint2));

    }
}

