package com.ctrip.framework.drc.replicator.container.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2022/3/29
 */
public class HeartBeatConfigurationTest {

    private HeartBeatConfiguration heartBeatConfiguration = HeartBeatConfiguration.getInstance();

    @Test
    public void testGray() {
        boolean res = heartBeatConfiguration.gray("123");
        Assert.assertTrue(res);
        res = heartBeatConfiguration.gray("1234");
        Assert.assertTrue(res);
    }

}