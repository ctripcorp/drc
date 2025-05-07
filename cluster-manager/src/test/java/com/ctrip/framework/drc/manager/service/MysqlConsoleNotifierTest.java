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
 * 2024/10/25 15:45
 */
public class MysqlConsoleNotifierTest {

    @InjectMocks
    private MysqlConsoleNotifier mysqlConsoleNotifier = new MysqlConsoleNotifier();

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
        Mockito.when(clusterManagerConfig.getConsoleRegionInfos()).thenReturn(regionInfoMap);
    }

    @Test
    public void testNotifyMasterChanged() throws Exception {
        Mockito.when(clusterManagerConfig.getConsoleBatchNotifySize()).thenReturn(2);
        mysqlConsoleNotifier.notifyMasterChanged("cluster1", "ip1");
        Assert.assertEquals(1, mysqlConsoleNotifier.getClusterMap().size());

        mysqlConsoleNotifier.notifyMasterChanged("cluster2", "ip2");
        Assert.assertEquals(0, mysqlConsoleNotifier.getClusterMap().size());
    }

    @Test
    public void testStartScheduleCheck() throws Exception {
        Mockito.when(clusterManagerConfig.getConsoleBatchNotifySize()).thenReturn(3);
        mysqlConsoleNotifier.afterPropertiesSet();

        Thread.sleep(100);

        mysqlConsoleNotifier.notifyMasterChanged("cluster1", "ip1");
        Assert.assertEquals(1, mysqlConsoleNotifier.getClusterMap().size());
        mysqlConsoleNotifier.notifyMasterChanged("cluster2", "ip2");
        Assert.assertEquals(2, mysqlConsoleNotifier.getClusterMap().size());

        Mockito.when(clusterManagerConfig.getCmBatchNotifyConsoleSwitch()).thenReturn(true);
        Thread.sleep(2000);
        Assert.assertEquals(0, mysqlConsoleNotifier.getClusterMap().size());
    }

}
