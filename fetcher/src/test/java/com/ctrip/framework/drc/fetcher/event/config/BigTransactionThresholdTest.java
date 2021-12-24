package com.ctrip.framework.drc.fetcher.event.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class BigTransactionThresholdTest {

    @Test
    public void getThreshold() {
        Assert.assertEquals(BigTransactionThreshold.getInstance().getThreshold(), 1001);
    }
}