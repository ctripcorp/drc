package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/5/18
 */
public class DefaultApplierMasterChooserManagerTest extends AbstractDbClusterTest {

    @InjectMocks
    private DefaultApplierMasterChooserManager applierMasterChooserManager = new DefaultApplierMasterChooserManager();

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        applierMasterChooserManager.initialize();
        applierMasterChooserManager.start();
    }

    @After
    public void tearDown() {
        super.tearDown();
        try {
            applierMasterChooserManager.stop();
            applierMasterChooserManager.dispose();
        } catch (Exception e) {
        }
    }

    @Test
    public void handleClusterChange() {
        String replicatorMha = dbCluster.getMhaName();
        String mha = dbCluster.getAppliers().get(0).getTargetMhaName();
        String name = dbCluster.getAppliers().get(0).getTargetMhaName();
        String idc = dbCluster.getAppliers().get(0).getTargetIdc();
        DefaultApplierMasterChooserManager.Key key1 = new DefaultApplierMasterChooserManager.Key(idc, mha, name, replicatorMha);
        DefaultApplierMasterChooserManager.Key key2 = new DefaultApplierMasterChooserManager.Key(idc, mha, name, replicatorMha);
        Assert.assertEquals(key1, key2);

        doNothing().when(currentMetaManager).addResource(anyString(), anyObject());

        Assert.assertEquals(0, applierMasterChooserManager.getApplierMasterChoosers().size());

        applierMasterChooserManager.handleClusterAdd(dbCluster);
        verify(currentMetaManager, times(1)).addResource(anyString(), anyObject());
        Assert.assertEquals(1, applierMasterChooserManager.getApplierMasterChoosers().size());

        ClusterComparator clusterComparator = new ClusterComparator(dbCluster, dbCluster);
        applierMasterChooserManager.handleClusterModified(clusterComparator);  // get from Map

        verify(currentMetaManager, times(1)).addResource(anyString(), anyObject());
        Assert.assertEquals(1, applierMasterChooserManager.getApplierMasterChoosers().size());

        applierMasterChooserManager.handleClusterDeleted(dbCluster);
        Assert.assertEquals(0, applierMasterChooserManager.getApplierMasterChoosers().size());
    }

    @Test
    public void handleClusterChangeWithUpdate() throws Exception {
        String replicatorMha = dbCluster.getMhaName();
        String mha = dbCluster.getAppliers().get(0).getTargetMhaName();
        String name = dbCluster.getAppliers().get(0).getTargetName();
        String idc = dbCluster.getAppliers().get(0).getTargetIdc();
        DefaultApplierMasterChooserManager.Key key1 = new DefaultApplierMasterChooserManager.Key(idc, mha, name, replicatorMha);
        DefaultApplierMasterChooserManager.Key key2 = new DefaultApplierMasterChooserManager.Key(idc, mha, name, replicatorMha);
        Assert.assertEquals(key1, key2);

        doNothing().when(currentMetaManager).addResource(anyString(), anyObject());

        Assert.assertEquals(0, applierMasterChooserManager.getApplierMasterChoosers().size());

        applierMasterChooserManager.handleClusterAdd(dbCluster);
        verify(currentMetaManager, times(1)).addResource(anyString(), anyObject());
        Assert.assertEquals(1, applierMasterChooserManager.getApplierMasterChoosers().size());

        ClusterComparator clusterComparator = new ClusterComparator(dbCluster, dbCluster);
        applierMasterChooserManager.handleClusterModified(clusterComparator);  // get from Map

        verify(currentMetaManager, times(1)).addResource(anyString(), anyObject());
        Assert.assertEquals(1, applierMasterChooserManager.getApplierMasterChoosers().size());

        DbCluster clone = MetaClone.clone(dbCluster);
        List<Applier> previousAppliers = new ArrayList<>();
        for (Applier a : dbCluster.getAppliers()) {
            previousAppliers.add(MetaClone.clone(a));
        }

        clone.getAppliers().clear();
        applierMasterChooserManager.handleClusterDeleted(clone);
        Assert.assertEquals(1, applierMasterChooserManager.getApplierMasterChoosers().size());

        applierMasterChooserManager.getApplierMasterChoosers().values().iterator().next().stop();
        dbCluster.getAppliers().addAll(previousAppliers);
        applierMasterChooserManager.handleClusterAdd(dbCluster);
        Assert.assertTrue(applierMasterChooserManager.getApplierMasterChoosers().values().iterator().next().isStarted());

    }
}