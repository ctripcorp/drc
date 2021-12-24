package com.ctrip.framework.drc.console.monitor.delay.config;

import org.junit.Assert;
import org.junit.Test;

public class FileConfigTest extends AbstractConfigTest {

    FileConfig fileConfig = new FileConfig();

    @Test
    public void testUpdateConfig() {
        if(existFile()) {
            fileConfig.updateConfig();
            Assert.assertNotNull(fileConfig.xml);
            System.out.println(fileConfig.xml);
        }
    }
}
