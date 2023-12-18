package com.ctrip.framework.drc.manager.ha;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.manager.ha.cluster.impl.InstanceStateController;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MQ;
import static com.ctrip.framework.drc.manager.AllTests.BACKUP_DAL_CLUSTER_ID;

/**
 * @Author limingdong
 * @create 2020/5/19
 */
public class DefaultStateChangeHandlerTest extends AbstractDbClusterTest {

    @InjectMocks
    private DefaultStateChangeHandler stateChangeHandler = new DefaultStateChangeHandler();

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private InstanceStateController instanceStateController;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        stateChangeHandler.initialize();
        stateChangeHandler.start();
    }

    @After
    public void tearDown() {
        try {
            super.tearDown();
            stateChangeHandler.stop();
            stateChangeHandler.dispose();
        } catch (Exception e) {
        }
    }

    @Test
    public void replicatorActiveElected() {
        when(currentMetaManager.hasCluster(CLUSTER_ID)).thenReturn(false);
        stateChangeHandler.replicatorActiveElected(CLUSTER_ID, newReplicator);
        verify(instanceStateController, times(0)).addReplicator(CLUSTER_ID, newReplicator);

        when(currentMetaManager.hasCluster(CLUSTER_ID)).thenReturn(true);
        when(currentMetaManager.getCluster(CLUSTER_ID)).thenReturn(null);
        stateChangeHandler.replicatorActiveElected(CLUSTER_ID, newReplicator);
        verify(instanceStateController, times(0)).addReplicator(CLUSTER_ID, newReplicator);


        when(currentMetaManager.getCluster(CLUSTER_ID)).thenReturn(dbCluster);
        when(currentMetaManager.getSurviveReplicators(CLUSTER_ID)).thenReturn(Lists.newArrayList(newReplicator));
        stateChangeHandler.replicatorActiveElected(CLUSTER_ID, newReplicator);
        verify(instanceStateController, times(1)).addReplicator(CLUSTER_ID, newReplicator);
    }

    @Test
    public void applierActiveElected() {
        when(currentMetaManager.hasCluster(CLUSTER_ID)).thenReturn(false);
        stateChangeHandler.applierActiveElected(CLUSTER_ID, newApplier);
        verify(instanceStateController, times(0)).addApplier(CLUSTER_ID, newApplier);

        when(currentMetaManager.hasCluster(CLUSTER_ID)).thenReturn(true);
        when(currentMetaManager.getCluster(CLUSTER_ID)).thenReturn(null);
        stateChangeHandler.applierActiveElected(CLUSTER_ID, newApplier);
        verify(instanceStateController, times(0)).addApplier(CLUSTER_ID, newApplier);


        when(currentMetaManager.getCluster(CLUSTER_ID)).thenReturn(dbCluster);
        when(currentMetaManager.getSurviveAppliers(CLUSTER_ID, RegistryKey.from(newApplier.getTargetName(), newApplier.getTargetMhaName()))).thenReturn(Lists.newArrayList(newApplier));
        stateChangeHandler.applierActiveElected(CLUSTER_ID, newApplier);
        verify(instanceStateController, times(1)).addApplier(CLUSTER_ID, newApplier);
    }

    @Test
    public void messengerActiveElected() {
        when(currentMetaManager.hasCluster(CLUSTER_ID)).thenReturn(false);
        stateChangeHandler.messengerActiveElected(CLUSTER_ID, newMessenger);
        verify(instanceStateController, times(0)).addMessenger(CLUSTER_ID, newMessenger);

        when(currentMetaManager.hasCluster(CLUSTER_ID)).thenReturn(true);
        when(currentMetaManager.getCluster(CLUSTER_ID)).thenReturn(null);
        stateChangeHandler.messengerActiveElected(CLUSTER_ID, newMessenger);
        verify(instanceStateController, times(0)).addMessenger(CLUSTER_ID, newMessenger);


        when(currentMetaManager.getCluster(CLUSTER_ID)).thenReturn(dbCluster);
        when(currentMetaManager.getSurviveMessengers(CLUSTER_ID, DRC_MQ)).thenReturn(Lists.newArrayList(newMessenger));
        stateChangeHandler.messengerActiveElected(CLUSTER_ID, newMessenger);
        verify(instanceStateController, times(1)).addMessenger(CLUSTER_ID, newMessenger);
    }

    @Test(expected = IllegalStateException.class)
    public void applierMasterChanged() {
        when(currentMetaManager.getActiveApplier(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID)).thenReturn(null);
        stateChangeHandler.applierMasterChanged(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, applierMaster);
        verify(instanceStateController, times(0)).applierMasterChange(CLUSTER_ID, applierMaster, newApplier);

        newApplier.setMaster(false);
        when(currentMetaManager.getActiveApplier(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID)).thenReturn(newApplier);
        stateChangeHandler.applierMasterChanged(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, applierMaster);

    }

    @Test
    public void applierMasterChangedRight() {
        newApplier.setMaster(true);
        when(currentMetaManager.getActiveApplier(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID)).thenReturn(newApplier);
        stateChangeHandler.applierMasterChanged(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, applierMaster);
        verify(instanceStateController, times(1)).applierMasterChange(CLUSTER_ID, applierMaster, newApplier);
    }

    @Test
    public void mysqlMasterChanged() {
        newApplier.setMaster(false);
        List<Applier> applierList = Lists.newArrayList(newApplier);
        when(currentMetaManager.getActiveAppliers(CLUSTER_ID)).thenReturn(applierList);
        stateChangeHandler.mysqlMasterChanged(CLUSTER_ID, mysqlMaster);
        verify(instanceStateController, times(0)).mysqlMasterChanged(CLUSTER_ID, mysqlMaster, applierList, newReplicator);

        newApplier.setMaster(true);
        applierList = Lists.newArrayList(newApplier);
        when(currentMetaManager.getActiveAppliers(CLUSTER_ID)).thenReturn(applierList);
        newReplicator.setMaster(true);
        when(currentMetaManager.getActiveReplicator(CLUSTER_ID)).thenReturn(newReplicator);
        stateChangeHandler.mysqlMasterChanged(CLUSTER_ID, mysqlMaster);
        verify(instanceStateController, times(1)).mysqlMasterChanged(CLUSTER_ID, mysqlMaster, applierList, newReplicator);

    }
}
