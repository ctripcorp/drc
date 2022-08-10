package com.ctrip.framework.drc.manager.ha.config;

import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2021/12/6
 */
public class DefaultClusterManagerConfigTest {

    private DefaultClusterManagerConfig clusterManagerConfig;

    @Before
    public void setUp() throws Exception {
        clusterManagerConfig = new DefaultClusterManagerConfig();
    }

    @Test
    public void getMigrationIdc() {
        Map<String, String> migrationMap = clusterManagerConfig.getMigrationIdc();
        Assert.assertEquals(1, migrationMap.size());
        Assert.assertEquals("shaxy", migrationMap.get("shaoy"));
    }

    @Test
    public void getMigrationBlackIps() {
        String blackIps = clusterManagerConfig.getMigrationBlackIps();
        Assert.assertTrue(blackIps.contains("127.0.0.1"));
        Assert.assertTrue(blackIps.contains("127.0.0.2"));
    }

    @Test
    public void getCmRegionInfos() {
        Map<String, RegionInfo> cmRegionInfos = clusterManagerConfig.getCmRegionInfos();
        Assert.assertEquals(1, cmRegionInfos.size());
        Assert.assertEquals("127.0.0.1", cmRegionInfos.get("sha").getMetaServerAddress());
    }

    @Test
    public void getConsoleRegionInfos() {
        Map<String, RegionInfo> consoleRegionInfos = clusterManagerConfig.getConsoleRegionInfos();
        Assert.assertEquals(1, consoleRegionInfos.size());
        Assert.assertEquals("127.0.0.2", consoleRegionInfos.get("sha").getMetaServerAddress());
    }

}
