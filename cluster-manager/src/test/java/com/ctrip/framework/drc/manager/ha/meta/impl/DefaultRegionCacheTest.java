package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.config.SourceProvider;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.collections.Sets;

import java.util.Map;
import java.util.Set;

/**
 * Created by jixinwang on 2022/8/11
 */
public class DefaultRegionCacheTest extends AbstractDbClusterTest {

    @InjectMocks
    private DefaultRegionCache regionCache;

    @Mock
    private SourceProvider sourceProvider;

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private DataCenterService dataCenterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Set<String> dcs = Sets.newSet("shaoy", "sharb");
        Map<String, Set<String>> regionIdcMapping = Maps.newHashMap();
        regionIdcMapping.put("sha", dcs);

        when(dataCenterService.getRegion()).thenReturn("sha");
        when(dataCenterService.getRegionIdcMapping()).thenReturn(regionIdcMapping);
        when(sourceProvider.getDc("shaoy")).thenReturn(drc.getDcs().get("shaoy"));
        when(sourceProvider.getDc("sharb")).thenReturn(drc.getDcs().get("sharb"));
        regionCache.doInitialize();
    }

    @Test
    public void getClusters() {
        Set<String> clusters = regionCache.getClusters();
        Assert.assertEquals(5, clusters.size());
    }

    @Test
    public void getBackupDcs() {
        Map<String, String> backupDcs = regionCache.getBackupDcs("integration-test.fxdrc");
        Assert.assertEquals(1, backupDcs.size());
    }

    @Test
    public void getCluster() {
        DbCluster dbCluster = regionCache.getCluster("integration-test.fxdrc");
        Assert.assertNotNull(dbCluster);
    }

    @Test
    public void randomRoute() {
        Route route = regionCache.randomRoute("integration-test.fxdrc", "sharb");
        Assert.assertNull(route);
    }
}
