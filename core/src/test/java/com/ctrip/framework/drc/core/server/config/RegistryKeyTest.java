package com.ctrip.framework.drc.core.server.config;

import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DOT;

/**
 * @Author limingdong
 * @create 2020/4/1
 */
public class RegistryKeyTest extends AbstractRegistryKey {

    @Test
    public void testEquals() {

        registryKey1 =  RegistryKey.from(CLUSTER_NAME, OY_MHA);
        registryKey2 =  RegistryKey.from(CLUSTER_NAME, OY_MHA);
        Assert.assertEquals(registryKey1, registryKey2);

        registryKey3 = RegistryKey.from(CLUSTER_NAME, RB_MHA);
        Assert.assertNotEquals(registryKey1, registryKey3);

        Assert.assertEquals(registryKey1, CLUSTER_NAME + DOT + OY_MHA);
        Assert.assertEquals(registryKey3, CLUSTER_NAME + DOT + RB_MHA);

        String targetMha = RegistryKey.getTargetMha(CLUSTER_NAME + DOT + OY_MHA + DOT + RB_MHA);
        Assert.assertEquals(RB_MHA, targetMha);

        try {
            RegistryKey.getTargetMha(CLUSTER_NAME + DOT + OY_MHA);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test(){
        Assert.assertEquals(CLUSTER_NAME + DOT + OY_MHA, RegistryKey.from(String.join(DOT, CLUSTER_NAME, OY_MHA, RB_MHA)).toString());
        Assert.assertEquals(CLUSTER_NAME + DOT + OY_MHA, RegistryKey.from(String.join(DOT, CLUSTER_NAME, OY_MHA, RB_MHA, "db1")).toString());
    }
}