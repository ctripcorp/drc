package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcService;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcServiceManager;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Map;
import java.util.concurrent.Executors;

import static com.ctrip.framework.drc.manager.AllTests.*;

/**
 * @Author limingdong
 * @create 2020/5/18
 */
public class MultiDcNotifierTest extends AbstractDbClusterTest {

    @InjectMocks
    private MultiDcNotifier multiDcNotifier = new MultiDcNotifier();

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private ClusterManagerMultiDcServiceManager clusterManagerMultiDcServiceManager;

    @Mock
    private ClusterManagerMultiDcService clusterManagerMultiDcService;

    @Mock
    public RegionCache regionMetaCache;

    @Mock
    public DataCenterService dataCenter;

    private Replicator replicator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        replicator = MetaClone.clone(dbCluster.getReplicators().get(0));
        replicator.setMaster(true);
        multiDcNotifier.setExecutors(Executors.newFixedThreadPool(1));

    }

    @Test
    public void replicatorActiveElected() throws InterruptedException {
        Map<String, RegionInfo> notEmpry = Maps.newConcurrentMap();
        RegionInfo regionInfo = new RegionInfo();
        regionInfo.setMetaServerAddress(LOCAL_IP);
        notEmpry.put(TARGET_REGION, regionInfo);
        when(config.getRegionInfos()).thenReturn(notEmpry);

        Map<String, String> backupDcs = Maps.newConcurrentMap();
        backupDcs.put(TARGET_DC, BACKUP_DAL_CLUSTER_ID);
        when(regionMetaCache.getBackupDcs(CLUSTER_ID)).thenReturn(backupDcs);

        when(clusterManagerMultiDcServiceManager.getOrCreate(LOCAL_IP)).thenReturn(clusterManagerMultiDcService);
        doNothing().when(clusterManagerMultiDcService).upstreamChange(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, replicator.getIp(), replicator.getApplierPort());

        when(dataCenter.getRegion(TARGET_DC)).thenReturn(TARGET_REGION);
        multiDcNotifier.replicatorActiveElected(CLUSTER_ID, null);
        verify(clusterManagerMultiDcService, times(0)).upstreamChange(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, replicator.getIp(), replicator.getApplierPort());

        multiDcNotifier.replicatorActiveElected(CLUSTER_ID, replicator);
        Thread.sleep(100);
        verify(clusterManagerMultiDcService, times(1)).upstreamChange(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, replicator.getIp(), replicator.getApplierPort());
    }
}
