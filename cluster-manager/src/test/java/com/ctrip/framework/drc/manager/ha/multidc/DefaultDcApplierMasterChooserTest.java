package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.framework.drc.manager.AllTests.BACKUP_DAL_CLUSTER_ID;
import static com.ctrip.framework.drc.manager.AllTests.TARGET_DC;

/**
 * @Author limingdong
 * @create 2020/5/18
 */
public class DefaultDcApplierMasterChooserTest extends AbstractDbClusterTest {

    private DefaultDcApplierMasterChooser defaultDcApplierMasterChooser;

    @Mock
    private MultiDcService multiDcService;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private ClusterManagerConfig clusterManagerConfig;

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("DefaultDcApplierMasterChooserTest");

    @Before
    public void setUp() throws Exception {
        super.setUp();

        defaultDcApplierMasterChooser = new DefaultDcApplierMasterChooser(TARGET_DC, CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, multiDcService, currentMetaManager, clusterManagerConfig, scheduledExecutorService);
    }

    @Test
    public void chooseApplierMaster() {
        newReplicator.setIp(LOCAL_IP);
        newReplicator.setApplierPort(backupPort);
        when(multiDcService.getActiveReplicator(TARGET_DC, BACKUP_DAL_CLUSTER_ID)).thenReturn(newReplicator);

        Pair<String, Integer> master = defaultDcApplierMasterChooser.chooseApplierMaster();
        Assert.assertNotNull(master);
        Assert.assertEquals(master.getKey(), LOCAL_IP);
        Assert.assertEquals(master.getValue().intValue(), backupPort);
    }
}