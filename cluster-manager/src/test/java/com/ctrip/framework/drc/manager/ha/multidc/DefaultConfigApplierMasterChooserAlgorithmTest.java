package com.ctrip.framework.drc.manager.ha.multidc;

import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.ctrip.framework.drc.manager.AllTests.BACKUP_DAL_CLUSTER_ID;
import static com.ctrip.framework.drc.manager.AllTests.TARGET_DC;

/**
 * @Author limingdong
 * @create 2021/12/17
 */
public class DefaultConfigApplierMasterChooserAlgorithmTest extends AbstractDbClusterTest {

    private DefaultConfigApplierMasterChooserAlgorithm configApplierMasterChooserAlgorithm;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private ClusterManagerConfig clusterManagerConfig;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        configApplierMasterChooserAlgorithm = new DefaultConfigApplierMasterChooserAlgorithm(TARGET_DC, CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, currentMetaManager, clusterManagerConfig, scheduledExecutorService);
    }

    @Test
    public void chooseFromConfig() {
        when(clusterManagerConfig.getApplierMaster(BACKUP_DAL_CLUSTER_ID + "." + TARGET_DC)).thenReturn(Pair.from(newReplicator.getIp(), newReplicator.getApplierPort()));
        Pair<String, Integer> master = configApplierMasterChooserAlgorithm.choose();
        Assert.assertEquals(master.getKey(), newReplicator.getIp());
        Assert.assertEquals(master.getValue(), newReplicator.getApplierPort());
    }

    @Test
    public void chooseFromFile() {
        when(clusterManagerConfig.getApplierMaster(BACKUP_DAL_CLUSTER_ID + "." + TARGET_DC)).thenReturn(null);
        Pair<String, Integer> master = configApplierMasterChooserAlgorithm.choose();
        Assert.assertNotNull(master);
        Assert.assertEquals(master.getKey(), "127.0.0.2");
        Assert.assertEquals(master.getValue().intValue(), 8413);
    }
}