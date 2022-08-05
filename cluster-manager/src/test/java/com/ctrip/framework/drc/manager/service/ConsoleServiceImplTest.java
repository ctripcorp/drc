package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.DcInfo;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Map;

import static com.ctrip.framework.drc.manager.AllTests.DC;
import static com.ctrip.framework.drc.manager.AllTests.TARGET_DC;

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

    private Map<String, DcInfo> dcInfoMap = Maps.newConcurrentMap();

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
        dcInfoMap.clear();
        dcInfoMap.put(DC, new DcInfo("http://127.0.0.1:8080"));
        when(clusterManagerConfig.getConsoleDcInofs()).thenReturn(dcInfoMap);
        when(dataCenterService.getDc()).thenReturn(DC);

        consoleService.replicatorActiveElected(CLUSTER_ID, null);
        verify(clusterManagerConfig, times(0)).getConsoleDcInofs();
        consoleService.replicatorActiveElected(CLUSTER_ID, newReplicator);
        verify(clusterManagerConfig, times(1)).getConsoleDcInofs();

        dcInfoMap.put(TARGET_DC, new DcInfo("http://127.0.0.1:8080"));
        consoleService.replicatorActiveElected(CLUSTER_ID, newReplicator);
        verify(clusterManagerConfig, times(2)).getConsoleDcInofs();


    }

    @Test
    public void mysqlMasterChanged() {


        dcInfoMap.clear();
        dcInfoMap.put(TARGET_DC, new DcInfo("http://127.0.0.1:8080"));
        when(clusterManagerConfig.getConsoleDcInofs()).thenReturn(dcInfoMap);
        when(dataCenterService.getDc()).thenReturn(DC);
        consoleService.mysqlMasterChanged(CLUSTER_ID, mysqlMaster);
        verify(clusterManagerConfig, times(1)).getConsoleDcInofs();

        consoleService.mysqlMasterChanged(CLUSTER_ID, mysqlMaster);
        verify(clusterManagerConfig, times(2)).getConsoleDcInofs();

        dcInfoMap.put(DC, new DcInfo("http://127.0.0.1:8080"));
        consoleService.mysqlMasterChanged(CLUSTER_ID, mysqlMaster);
        verify(clusterManagerConfig, times(3)).getConsoleDcInofs();
    }
    
}
