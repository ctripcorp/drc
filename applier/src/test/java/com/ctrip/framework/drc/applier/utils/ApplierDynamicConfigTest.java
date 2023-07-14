package com.ctrip.framework.drc.applier.utils;

import org.junit.Assert;
import org.junit.Test;


/**
 * Created by jixinwang on 2023/4/7
 */
public class ApplierDynamicConfigTest {

    @Test
    public void getLwmToleranceTime() {
        long res = ApplierDynamicConfig.getInstance().getLwmToleranceTime();
        Assert.assertEquals(5 * 60 * 1000, res);
    }

    @Test
    public void getFirstLwmToleranceTime() {
        long res = ApplierDynamicConfig.getInstance().getFirstLwmToleranceTime();
        Assert.assertEquals(20 * 60 * 1000, res);
    }
}