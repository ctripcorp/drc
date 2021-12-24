package com.ctrip.framework.drc.console.monitor.delay.config;

import org.junit.Assert;
import org.junit.Test;

public class CompositeConfigTest {

    @Test
    public void testUpdateConfig() {
        CompositeConfig compositeConfig = new CompositeConfig();
        compositeConfig.initConfigs();
        compositeConfig.updateConfig();
        Assert.assertNull(compositeConfig.xml);

        compositeConfig.addConfig(new FileConfig());
        compositeConfig.updateConfig();
        Assert.assertNotNull(compositeConfig.xml);
    }
}
