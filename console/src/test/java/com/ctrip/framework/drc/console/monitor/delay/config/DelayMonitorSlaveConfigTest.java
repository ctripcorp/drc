package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.RouteUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.MYSQL_DELAY_MESUREMENT;
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

    @Test
    public void testClone() {
        DelayMonitorSlaveConfig config = new DelayMonitorSlaveConfig();
        config.setDc("srcDc");
        config.setDestDc("destDc");
        config.setCluster("clusterName");
        config.setMha("srcMhaName");
        config.setDestMha("destMhaName");
        config.setRegistryKey(RegistryKey.from("clusterName", "mhaName"));
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", 8080);
        config.setEndpoint(endpoint);
        config.setMeasurement(MYSQL_DELAY_MESUREMENT);
        config.setRouteInfo("PROXY://10.10.10.10:80 PROXYTLS://10.10.10.11:443,PROXYTLS://10.10.10.12:443");

        DelayMonitorSlaveConfig cloneConfig = config.clone();
        Assert.assertEquals(config, cloneConfig);
    }
}
