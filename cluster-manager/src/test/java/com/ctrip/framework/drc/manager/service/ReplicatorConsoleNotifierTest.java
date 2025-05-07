package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2024/10/25 16:19
 */
public class ReplicatorConsoleNotifierTest {

    @InjectMocks
    private ReplicatorConsoleNotifier replicatorConsoleNotifier = new ReplicatorConsoleNotifier();

    @Mock
    private DataCenterService dataCenterService;

    @Mock
    private ClusterManagerConfig clusterManagerConfig;


    private Map<String, RegionInfo> regionInfoMap = Maps.newConcurrentMap();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(dataCenterService.getRegion()).thenReturn("sha");

        regionInfoMap.put("sha", new RegionInfo("http://127.0.0.1:8080"));
        regionInfoMap.put("sgp", new RegionInfo("http://127.0.0.1:8080"));
        regionInfoMap.put("fra", new RegionInfo("http://127.0.0.1:8080"));
        Mockito.when(clusterManagerConfig.getConsoleRegionInfos()).thenReturn(regionInfoMap);
    }

    @Test
    public void testNotifyMasterChanged() throws Exception {
        Mockito.when(clusterManagerConfig.getConsoleBatchNotifySize()).thenReturn(2);
        replicatorConsoleNotifier.notifyMasterChanged("cluster1", "ip1");
        Assert.assertEquals(1, replicatorConsoleNotifier.getClusterMap().size());

        replicatorConsoleNotifier.notifyMasterChanged("cluster2", "ip2");
        Assert.assertEquals(0, replicatorConsoleNotifier.getClusterMap().size());
    }

    @Test
    public void testStartScheduleCheck() throws Exception {
        Mockito.when(clusterManagerConfig.getConsoleBatchNotifySize()).thenReturn(3);
        replicatorConsoleNotifier.afterPropertiesSet();
        Thread.sleep(100);

        replicatorConsoleNotifier.notifyMasterChanged("cluster1", "ip1");
        Assert.assertEquals(1, replicatorConsoleNotifier.getClusterMap().size());
        replicatorConsoleNotifier.notifyMasterChanged("cluster2", "ip2");
        Assert.assertEquals(2, replicatorConsoleNotifier.getClusterMap().size());

        Thread.sleep(2000);
        Assert.assertEquals(0, replicatorConsoleNotifier.getClusterMap().size());
    }
}
