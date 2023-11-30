package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.zk.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;

/**
 * Created by jixinwang on 2022/11/16
 */
public class MessengerInstanceManagerTest extends AbstractDbClusterTest {

    @InjectMocks
    private MessengerInstanceManager messengerInstanceManager = new MessengerInstanceManager();

    @Mock
    private ZkClient zkClient;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private RegionCache regionCache;

    @Mock
    private InstanceStateController instanceStateController;

    @Mock
    private InstanceActiveElectAlgorithmManager instanceActiveElectAlgorithmManager;

    @Mock
    protected ClusterManagerConfig clusterManagerConfig;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

//        super.setUp();
//        when(zkClient.get()).thenReturn(curatorFramework);
//        when(currentMetaManager.watchMessengerIfNotWatched(anyString())).thenReturn(true);
//        when(instanceActiveElectAlgorithmManager.get(anyString())).thenReturn(new DefaultInstanceActiveElectAlgorithm());
//        when(regionCache.getCluster(anyString())).thenReturn(dbCluster);
//
//        Messenger messenger = dbCluster.getMessengers().get(0);
//        zookeeperValue.setPort(messenger.getPort());
//        zookeeperValue.setIp(messenger.getIp());
//        messengerInstanceManager.initialize();
//        messengerInstanceManager.start();
    }

    @After
    public void tearDown() {
//        try {
//            if (persistentNode != null) {
//                persistentNode.close();
//            }
//            messengerInstanceManager.stop();
//            messengerInstanceManager.dispose();
//        } catch (Exception e) {
//
//        }
    }


    @Test
    public void handleClusterAdded() throws InterruptedException {
        messengerInstanceManager.update(new NodeAdded<>(dbCluster), null);
        verify(instanceStateController, times(1)).registerMessenger(anyString(), anyObject());
    }

    @Test
    public void handleClusterDelete() {
        messengerInstanceManager.update(new NodeDeleted<>(dbCluster), null);
        verify(instanceStateController, times(1)).removeMessenger(dbCluster.getId(), dbCluster.getMessengers().get(0), true);
    }

    @Test
    public void handleClusterModify() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        newMessenger.setIp("12.21.12.21");
        newMessenger.setPort(4321);
        cloneDbCluster.getMessengers().clear();
        cloneDbCluster.getMessengers().add(newMessenger);
        ClusterComparator clusterComparator = new ClusterComparator(dbCluster, cloneDbCluster);
        clusterComparator.compare();
        messengerInstanceManager.update(clusterComparator, null);
        verify(instanceStateController, times(1)).removeMessenger(dbCluster.getId(), dbCluster.getMessengers().get(0), true);
    }

    @Test
    public void handleClusterModified() {
        String clusterId = "test_id";

        Dbs dbs = new Dbs();
        Db db = new Db();
        dbs.addDb(db);

        DbCluster current = new DbCluster();
        current.setDbs(dbs);
        current.setId(clusterId);
        Messenger messenger = new Messenger();
        messenger.setIp("127.0.0.1");
        messenger.setPort(8080);
        current.addMessenger(messenger);

        DbCluster future = new DbCluster();
        future.setDbs(dbs);
        future.setId(clusterId);
        Messenger messenger2 = new Messenger();
        messenger2.setProperties("test_property");
        messenger2.setIp("127.0.0.1");
        messenger2.setPort(8080);
        future.addMessenger(messenger2);

        ClusterComparator comparator = new ClusterComparator(current, future);
        comparator.compare();

        Mockito.when(currentMetaManager.getActiveMessenger(Mockito.anyString())).thenReturn(messenger);
        Mockito.when(clusterManagerConfig.checkApplierProperty()).thenReturn(true);

        messengerInstanceManager.handleClusterModified(comparator);

        Mockito.verify(instanceStateController, times(1)).messengerPropertyChange(clusterId, messenger2);
    }
}
