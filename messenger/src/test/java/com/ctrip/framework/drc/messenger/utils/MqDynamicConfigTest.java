package com.ctrip.framework.drc.messenger.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by shiruixin
 * 2024/11/8 16:57
 */
public class MqDynamicConfigTest {
    @Test
    public void getLwmToleranceTime() {
        long res = MqDynamicConfig.getInstance().getLwmToleranceTime();
        Assert.assertEquals(5 * 60 * 1000, res);
    }

    @Test
    public void getFirstLwmToleranceTime() {
        long res = MqDynamicConfig.getInstance().getFirstLwmToleranceTime();
        Assert.assertEquals(20 * 60 * 1000, res);
    }

    @Test
    public void getBigRowsEventSize() {
        int res = MqDynamicConfig.getInstance().getBigRowsEventSize();
        Assert.assertEquals(100, res);
    }
}