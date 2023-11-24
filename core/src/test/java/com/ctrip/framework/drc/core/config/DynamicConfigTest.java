package com.ctrip.framework.drc.core.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2022/11/29
 */
public class DynamicConfigTest {

    private DynamicConfig dynamicConfig = DynamicConfig.getInstance();

    @Test
    public void getIndependentEmbeddedMySQLSwitch() {
        boolean switchRes = dynamicConfig.getIndependentEmbeddedMySQLSwitch("registryKey");
        Assert.assertTrue(switchRes);

        switchRes = dynamicConfig.getIndependentEmbeddedMySQLSwitch("registryKey-mock");
        Assert.assertFalse(switchRes);

        switchRes = dynamicConfig.getDisableSnapshotCacheSwitch();
        Assert.assertTrue(switchRes);
    }
}