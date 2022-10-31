package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Map;

import static com.ctrip.framework.drc.manager.AllTests.*;

/**
 * @Author limingdong
 * @create 2020/7/3
 */
public class ConsoleServiceImplTest extends AbstractDbClusterTest {

    @InjectMocks
    private ConsoleServiceImpl consoleService = new ConsoleServiceImpl();

    @Mock
    private DataCenterService dataCenterService;

    @Mock
    private ClusterManagerConfig clusterManagerConfig;

    private Map<String, RegionInfo> regionInfoMap = Maps.newConcurrentMap();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        try {
            super.tearDown();
        } catch (Exception e) {
        }
    }

    @Test
    public void replicatorActiveElected() {
        regionInfoMap.clear();
        regionInfoMap.put(REGION, new RegionInfo("http://127.0.0.1:8080"));
        when(clusterManagerConfig.getConsoleRegionInfos()).thenReturn(regionInfoMap);
        when(dataCenterService.getRegion()).thenReturn(REGION);

        consoleService.replicatorActiveElected(CLUSTER_ID, null);
        verify(clusterManagerConfig, times(0)).getConsoleRegionInfos();
        consoleService.messengerActiveElected(CLUSTER_ID, newReplicator);
        verify(clusterManagerConfig, times(1)).getConsoleRegionInfos();

        regionInfoMap.put(TARGET_DC, new RegionInfo("http://127.0.0.1:8080"));
        consoleService.messengerActiveElected(CLUSTER_ID, newReplicator);
        verify(clusterManagerConfig, times(2)).getConsoleRegionInfos();


    }

    @Test
    public void mysqlMasterChanged() {


        regionInfoMap.clear();
        regionInfoMap.put(TARGET_DC, new RegionInfo("http://127.0.0.1:8080"));
        when(clusterManagerConfig.getConsoleRegionInfos()).thenReturn(regionInfoMap);
        when(dataCenterService.getRegion()).thenReturn(REGION);
        consoleService.mysqlMasterChanged(CLUSTER_ID, mysqlMaster);
        verify(clusterManagerConfig, times(1)).getConsoleRegionInfos();

        consoleService.mysqlMasterChanged(CLUSTER_ID, mysqlMaster);
        verify(clusterManagerConfig, times(2)).getConsoleRegionInfos();

        regionInfoMap.put(DC, new RegionInfo("http://127.0.0.1:8080"));
        consoleService.mysqlMasterChanged(CLUSTER_ID, mysqlMaster);
        verify(clusterManagerConfig, times(3)).getConsoleRegionInfos();
    }

}
