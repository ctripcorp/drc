package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

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
    public void handleClusterAdded() {
        String mha = dbCluster.getAppliers().get(0).getTargetMhaName();
        String idc = dbCluster.getAppliers().get(0).getTargetIdc();
        DefaultApplierMasterChooserManager.Key key1 = new DefaultApplierMasterChooserManager.Key(mha, idc);
        DefaultApplierMasterChooserManager.Key key2 = new DefaultApplierMasterChooserManager.Key(mha, idc);
        Assert.assertEquals(key1, key2);

        doNothing().when(currentMetaManager).addResource(anyString(), anyObject());

        applierMasterChooserManager.handleClusterAdd(dbCluster);
        verify(currentMetaManager, times(1)).addResource(anyString(), anyObject());

    }
}