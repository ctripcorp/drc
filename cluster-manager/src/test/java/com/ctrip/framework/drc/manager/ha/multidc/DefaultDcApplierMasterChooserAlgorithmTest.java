package com.ctrip.framework.drc.manager.ha.multidc;

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
 * @create 2020/5/18
 */
public class DefaultDcApplierMasterChooserAlgorithmTest extends AbstractDbClusterTest {

    private DefaultDcApplierMasterChooserAlgorithm defaultDcApplierMasterChooserAlgorithm;

    @Mock
    private MultiDcService multiDcService;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        defaultDcApplierMasterChooserAlgorithm = new DefaultDcApplierMasterChooserAlgorithm(TARGET_DC, CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, currentMetaManager, multiDcService, scheduledExecutorService);
    }

    @Test
    public void chooseNull() {
        Pair<String, Integer> master = defaultDcApplierMasterChooserAlgorithm.choose();
        Assert.assertNull(master);
    }

    @Test
    public void choose() {
        newReplicator.setIp(LOCAL_IP);
        newReplicator.setApplierPort(backupPort);
        when(multiDcService.getActiveReplicator(TARGET_DC, BACKUP_DAL_CLUSTER_ID)).thenReturn(newReplicator);
        Pair<String, Integer> master = defaultDcApplierMasterChooserAlgorithm.choose();
        Assert.assertNotNull(master);
        Assert.assertEquals(master.getKey(), LOCAL_IP);
        Assert.assertEquals(master.getValue().intValue(), backupPort);
    }
}