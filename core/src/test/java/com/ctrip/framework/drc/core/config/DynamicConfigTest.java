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

    @Test
    public void getScaleOut() {
        long registryKey = dynamicConfig.getBinlogScaleOutNum("unit_test_scaleout", 80);
        Assert.assertEquals(200, registryKey);
        registryKey = dynamicConfig.getBinlogScaleOutNum("unit_test_scaleout2", 80);
        Assert.assertEquals(43, registryKey);

        registryKey = dynamicConfig.getBinlogScaleOutNum("unit_test_scaleout3", 80);
        Assert.assertEquals(1, registryKey);


        // use default
        registryKey = dynamicConfig.getBinlogScaleOutNum("not_defined_registry_key", 80);
        Assert.assertEquals(80, registryKey);


        registryKey = dynamicConfig.getBinlogScaleOutNum("not_defined_registry_key", -1);
        Assert.assertEquals(1, registryKey);
    }
}