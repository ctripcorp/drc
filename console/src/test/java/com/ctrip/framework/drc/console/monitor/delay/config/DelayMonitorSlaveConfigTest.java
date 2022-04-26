package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jixinwang on 2022/4/26
 */
public class DelayMonitorSlaveConfigTest {

    @Test
    public void testNotEquals() {
        DelayMonitorSlaveConfig oldConfig = new DelayMonitorSlaveConfig();
        oldConfig.setEndpoint(new DefaultEndPoint("10.1.3.4", 8383));

        DelayMonitorSlaveConfig newConfig = new DelayMonitorSlaveConfig();
        newConfig.setEndpoint(new DefaultEndPoint("10.1.3.5", 8383));

        assertNotEquals(oldConfig, newConfig);
    }

    @Test
    public void testEquals() {
        DelayMonitorSlaveConfig oldConfig = new DelayMonitorSlaveConfig();
        oldConfig.setEndpoint(new DefaultEndPoint("10.1.3.4", 8383));

        DelayMonitorSlaveConfig newConfig = new DelayMonitorSlaveConfig();
        newConfig.setEndpoint(new DefaultEndPoint("10.1.3.4", 8383));

        assertEquals(oldConfig, newConfig);
    }
}
