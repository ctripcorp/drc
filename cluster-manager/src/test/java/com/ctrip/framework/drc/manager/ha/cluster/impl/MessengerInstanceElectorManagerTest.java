package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

/**
 * Created by jixinwang on 2022/11/15
 */
public class MessengerInstanceElectorManagerTest extends AbstractDbClusterTest {

    @InjectMocks
    private MessengerInstanceElectorManager messengerInstanceElectorManager = new MessengerInstanceElectorManager();

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


    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(zkClient.get()).thenReturn(curatorFramework);
        when(currentMetaManager.watchMessengerIfNotWatched(anyString())).thenReturn(true);
        when(instanceActiveElectAlgorithmManager.get(anyString())).thenReturn(new DefaultInstanceActiveElectAlgorithm());
        when(regionCache.getCluster(anyString())).thenReturn(dbCluster);

        Messenger messenger = dbCluster.getMessengers().get(0);
        zookeeperValue.setPort(messenger.getPort());
        zookeeperValue.setIp(messenger.getIp());
        messengerInstanceElectorManager.initialize();
        messengerInstanceElectorManager.start();
    }

    @After
    public void tearDown() {
        try {
            if (persistentNode != null) {
                persistentNode.close();
            }
            messengerInstanceElectorManager.stop();
            messengerInstanceElectorManager.dispose();
        } catch (Exception e) {

        }
    }

    @Test
    public void handleClusterAdded() throws InterruptedException {
        messengerInstanceElectorManager.update(new NodeAdded<>(dbCluster), null);

        persistentNode = new PersistentNode(curatorFramework, CreateMode.EPHEMERAL, false, messengerInstanceElectorManager.getLeaderPath(CLUSTER_ID + "." + SystemConfig.DRC_MQ) + "/leader", Codec.DEFAULT.encodeAsBytes(zookeeperValue));
        persistentNode.start();

        Thread.sleep(1000);
        verify(currentMetaManager, times(1)).setSurviveMessengers(anyString(), anyObject(), anyObject());
    }

    @Test
    public void handleClusterDelete() {
        messengerInstanceElectorManager.update(new NodeDeleted<>(dbCluster), null);
        verify(instanceStateController, times(0)).removeMessenger(anyString(), anyObject());
    }

    @Test
    public void handleClusterModify() {
        messengerInstanceElectorManager.update(new NodeDeleted<>(dbCluster), null);
        verify(instanceStateController, times(0)).removeMessenger(anyString(), anyObject());
    }
}
