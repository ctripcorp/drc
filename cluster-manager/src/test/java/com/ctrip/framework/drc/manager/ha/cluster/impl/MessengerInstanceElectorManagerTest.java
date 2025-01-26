package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MQ;

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

        persistentNode = new PersistentNode(curatorFramework, CreateMode.EPHEMERAL, false, messengerInstanceElectorManager.getLeaderPath(RegistryKey.from(CLUSTER_ID, DRC_MQ)) + "/leader", Codec.DEFAULT.encodeAsBytes(zookeeperValue));
        persistentNode.start();

        Thread.sleep(1500);
        verify(currentMetaManager, times(1)).setSurviveMessengers(anyString(), anyString(), anyObject(), anyObject());
    }

    @Test
    public void handleClusterDelete() {
        messengerInstanceElectorManager.update(new NodeDeleted<>(dbCluster), null);
        verify(instanceStateController, times(0)).removeMessenger(dbCluster.getId(), dbCluster.getMessengers().get(0), true);
    }

    @Test
    public void handleClusterModify() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        newMessenger.setIp("12.21.12.21");
        newMessenger.setPort(4321);
        newMessenger.setApplyMode(ApplyMode.mq.getType());
        cloneDbCluster.getMessengers().add(newMessenger);
        ClusterComparator clusterComparator = new ClusterComparator(dbCluster, cloneDbCluster);
        clusterComparator.compare();
        messengerInstanceElectorManager.update(clusterComparator, null);
        verify(instanceStateController, times(0)).removeMessenger(dbCluster.getId(), dbCluster.getMessengers().get(0), true);
    }

    @Test
    public void testGetMessenger() {
        String clusterId = "clusterId";
        DbCluster dbCluster = new DbCluster(clusterId)
                .addMessenger(new Messenger().setIp("db_messenger_ip_1").setPort(8080).setIncludedDbs("db1").setApplyMode(ApplyMode.mq.getType()))
                .addMessenger(new Messenger().setIp("db_messenger_ip_2").setPort(8080).setIncludedDbs("db1").setApplyMode(ApplyMode.mq.getType()));
        when(regionCache.getCluster(clusterId)).thenReturn(dbCluster);

        Assert.assertNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_1", 8080, null, ApplyMode.mq));
        Assert.assertNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_1", 8080, "db2", ApplyMode.mq));
        Assert.assertNotNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_1", 8080, "db1", ApplyMode.mq));


        dbCluster = new DbCluster(clusterId)
                .addMessenger(new Messenger().setIp("db_messenger_ip_1").setPort(8080).setIncludedDbs(null).setApplyMode(ApplyMode.mq.getType()))
                .addMessenger(new Messenger().setIp("db_messenger_ip_2").setPort(8080).setIncludedDbs(null).setApplyMode(ApplyMode.mq.getType()));
        when(regionCache.getCluster(clusterId)).thenReturn(dbCluster);

        Assert.assertNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_1", 8080, "db1", ApplyMode.mq));
        Assert.assertNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_3", 8080, null, ApplyMode.mq));
        Assert.assertNotNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_1", 8080, null, ApplyMode.mq));


        dbCluster = new DbCluster(clusterId)
                .addMessenger(new Messenger().setIp("db_messenger_ip_1").setPort(8080).setIncludedDbs(null).setApplyMode(ApplyMode.mq.getType()))
                .addMessenger(new Messenger().setIp("db_messenger_ip_2").setPort(8080).setIncludedDbs(null).setApplyMode(ApplyMode.mq.getType()))
                .addMessenger(new Messenger().setIp("db_messenger_ip_1").setPort(8080).setIncludedDbs(null).setApplyMode(ApplyMode.kafka.getType()))
                .addMessenger(new Messenger().setIp("db_messenger_ip_3").setPort(8080).setIncludedDbs(null).setApplyMode(ApplyMode.kafka.getType()));
        when(regionCache.getCluster(clusterId)).thenReturn(dbCluster);

        Assert.assertNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_1", 8080, "db1", ApplyMode.mq));
        Assert.assertNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_3", 8080, null, ApplyMode.mq));
        Assert.assertNotNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_1", 8080, null, ApplyMode.mq));
        Assert.assertNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_1", 8080, "db1", ApplyMode.kafka));
        Assert.assertNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_2", 8080, null, ApplyMode.kafka));
        Assert.assertNotNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_1", 8080, null, ApplyMode.kafka));
        Assert.assertNotNull(messengerInstanceElectorManager.getMessenger(clusterId, "db_messenger_ip_3", 8080, null, ApplyMode.kafka));
    }
}
