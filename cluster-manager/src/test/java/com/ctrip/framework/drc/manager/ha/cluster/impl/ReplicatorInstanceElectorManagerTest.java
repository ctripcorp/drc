package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Replicator;
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
 * @Author limingdong
 * @create 2020/5/12
 */
public class ReplicatorInstanceElectorManagerTest extends AbstractDbClusterTest {

    @InjectMocks
    private ReplicatorInstanceElectorManager replicatorInstanceElectorManager = new ReplicatorInstanceElectorManager();

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
        when(currentMetaManager.watchReplicatorIfNotWatched(anyString())).thenReturn(true);
        when(instanceActiveElectAlgorithmManager.get(anyString())).thenReturn(new DefaultInstanceActiveElectAlgorithm());
        when(regionCache.getCluster(anyString())).thenReturn(dbCluster);

        Replicator replicator = dbCluster.getReplicators().get(0);
        zookeeperValue.setPort(replicator.getPort());
        zookeeperValue.setIp(replicator.getIp());
        replicatorInstanceElectorManager.initialize();
        replicatorInstanceElectorManager.start();
    }

    @After
    public void tearDown() {
        try {
            if (persistentNode != null) {
                persistentNode.close();
            }
            replicatorInstanceElectorManager.stop();
            replicatorInstanceElectorManager.dispose();
        } catch (Exception e) {

        }
    }

    @Test
    public void handleClusterAdded() throws InterruptedException {
        replicatorInstanceElectorManager.update(new NodeAdded<>(dbCluster), null);

        persistentNode = new PersistentNode(curatorFramework, CreateMode.EPHEMERAL, false, replicatorInstanceElectorManager.getLeaderPath(CLUSTER_ID) + "/leader", Codec.DEFAULT.encodeAsBytes(zookeeperValue));
        persistentNode.start();

        Thread.sleep(1000);
        verify(currentMetaManager, times(1)).setSurviveReplicators(anyString(), anyObject(), anyObject());
    }

    @Test
    public void handleClusterDelete() {
        replicatorInstanceElectorManager.update(new NodeDeleted<>(dbCluster), null);
        verify(instanceStateController, times(0)).removeReplicator(anyString(), anyObject());
    }

    @Test
    public void handleClusterModify() {
        replicatorInstanceElectorManager.update(new NodeDeleted<>(dbCluster), null);
        verify(instanceStateController, times(0)).removeReplicator(anyString(), anyObject());
    }
}
