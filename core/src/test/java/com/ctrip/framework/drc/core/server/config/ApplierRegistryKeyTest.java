package com.ctrip.framework.drc.core.server.config;

import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DOT;

/**
 * @Author limingdong
 * @create 2020/4/1
 */
public class ApplierRegistryKeyTest extends AbstractRegistryKey {

    @Test
    public void testEquals() {

        registryKey1 =  ApplierRegistryKey.from(OY_MHA, CLUSTER_NAME, RB_MHA);
        registryKey2 =  ApplierRegistryKey.from(OY_MHA, CLUSTER_NAME, RB_MHA);
        Assert.assertEquals(registryKey1, registryKey2);

        registryKey3 =  ApplierRegistryKey.from(RB_MHA, CLUSTER_NAME, OY_MHA);
        Assert.assertNotEquals(registryKey1, registryKey3);

        Assert.assertEquals(registryKey1, CLUSTER_NAME + DOT + OY_MHA + DOT + RB_MHA);
        Assert.assertEquals(registryKey3, CLUSTER_NAME + DOT + RB_MHA + DOT + OY_MHA);

    }

}