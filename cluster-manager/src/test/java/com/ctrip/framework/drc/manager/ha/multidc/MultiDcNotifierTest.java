package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.ha.meta.DcInfo;
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

import static com.ctrip.framework.drc.manager.AllTests.BACKUP_DAL_CLUSTER_ID;
import static com.ctrip.framework.drc.manager.AllTests.TARGET_DC;

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
    public DcCache dcMetaCache;

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
        Map<String, DcInfo> notEmpry = Maps.newConcurrentMap();
        DcInfo dcInfo = new DcInfo();
        dcInfo.setMetaServerAddress(LOCAL_IP);
        notEmpry.put(TARGET_DC, dcInfo);
        when(config.getDcInofs()).thenReturn(notEmpry);

        Map<String, String> backupDcs = Maps.newConcurrentMap();
        backupDcs.put(TARGET_DC, BACKUP_DAL_CLUSTER_ID);
        when(dcMetaCache.getBackupDcs(CLUSTER_ID)).thenReturn(backupDcs);

        when(clusterManagerMultiDcServiceManager.getOrCreate(LOCAL_IP)).thenReturn(clusterManagerMultiDcService);
        doNothing().when(clusterManagerMultiDcService).upstreamChange(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, replicator.getIp(), replicator.getApplierPort());

        multiDcNotifier.replicatorActiveElected(CLUSTER_ID, null);
        verify(clusterManagerMultiDcService, times(0)).upstreamChange(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, replicator.getIp(), replicator.getApplierPort());

        multiDcNotifier.replicatorActiveElected(CLUSTER_ID, replicator);
        Thread.sleep(100);
        verify(clusterManagerMultiDcService, times(1)).upstreamChange(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, replicator.getIp(), replicator.getApplierPort());
    }
}