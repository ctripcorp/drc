package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.DcInfo;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcService;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcServiceManager;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Map;

import static com.ctrip.framework.drc.manager.AllTests.BACKUP_DAL_CLUSTER_ID;
import static com.ctrip.framework.drc.manager.AllTests.TARGET_DC;

/**
 * @Author limingdong
 * @create 2020/5/18
 */
public class DefaultMultiDcServiceTest extends AbstractDbClusterTest {

    @InjectMocks
    private DefaultMultiDcService multiDcService = new DefaultMultiDcService();

    @Mock
    private ClusterManagerMultiDcServiceManager clusterManagerMultiDcServiceManager;

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private ClusterManagerMultiDcService clusterManagerMultiDcService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void getActiveReplicatorNullAndRetry() {
        Map<String, DcInfo> empry = Maps.newConcurrentMap();
        when(config.getDcInofs()).thenReturn(empry);
        Map<String, String> migrationIdc = Maps.newHashMap();
        migrationIdc.put(TARGET_DC, "shaxy");
        when(config.getMigrationIdc()).thenReturn(migrationIdc);
        Replicator replicator = multiDcService.getActiveReplicator(TARGET_DC, BACKUP_DAL_CLUSTER_ID);
        Assert.assertNull(replicator);
        verify(config, times(1)).getMigrationIdc();
    }

    @Test
    public void getActiveReplicatorNotNull() {
        newReplicator.setIp(LOCAL_IP);
        newReplicator.setPort(backupPort);

        Map<String, DcInfo> notEmpry = Maps.newConcurrentMap();
        DcInfo dcInfo = new DcInfo();
        dcInfo.setMetaServerAddress(LOCAL_IP);
        notEmpry.put(TARGET_DC, dcInfo);
        when(config.getDcInofs()).thenReturn(notEmpry);
        when(clusterManagerMultiDcServiceManager.getOrCreate(LOCAL_IP)).thenReturn(clusterManagerMultiDcService);
        when(clusterManagerMultiDcService.getActiveReplicator(BACKUP_DAL_CLUSTER_ID)).thenReturn(newReplicator);

        Replicator replicator = multiDcService.getActiveReplicator(TARGET_DC, BACKUP_DAL_CLUSTER_ID);
        Assert.assertNotNull(replicator);
        Assert.assertEquals(replicator.getIp(), LOCAL_IP);
        Assert.assertEquals(replicator.getPort().intValue(), backupPort);
    }
}