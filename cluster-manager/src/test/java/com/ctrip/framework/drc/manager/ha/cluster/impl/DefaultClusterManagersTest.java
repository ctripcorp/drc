package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterManager;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServers;
import com.ctrip.framework.drc.manager.ha.cluster.RemoteClusterServerFactory;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeModified;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.doReturn;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class DefaultClusterManagersTest extends AbstractDbClusterTest {

    @InjectMocks
    private ClusterServers clusterServers = new DefaultClusterManagers();

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private ZkClient zkClient;

    @Mock
    private ClusterManager currentServer;

    @Mock
    private RemoteClusterServerFactory<ClusterManager> remoteClusterServerFactory;

    @Mock
    private Observer observer;

    private ClusterServerInfo clusterServerInfo;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(zkClient.get()).thenReturn(curatorFramework);
        when(currentServer.getServerId()).thenReturn(CLUSTER_ID);
        when(config.getClusterServersRefreshMilli()).thenReturn(10);
        when(config.getClusterServerIp()).thenReturn(IP);
        when(config.getClusterServerPort()).thenReturn(PORT);

        clusterServerInfo = new ClusterServerInfo(IP, PORT);
        clusterServers.addObserver(observer);

        clusterServers.initialize();
        clusterServers.start();
    }

    @After
    public void tearDown() {
        try {
            super.tearDown();
            clusterServers.stop();
            clusterServers.dispose();
        } catch (Exception e) {
        }
    }

    @Test
    public void childrenChange() throws InterruptedException {
        when(remoteClusterServerFactory.createClusterServer(anyString(), anyObject())).thenReturn(currentServer);
        when(currentServer.getClusterInfo()).thenReturn(clusterServerInfo);
        String serverPath = ClusterZkConfig.getClusterManagerRegisterPath() + "/" + CLUSTER_ID;
        persistentNode = new PersistentNode(curatorFramework, CreateMode.EPHEMERAL, false, serverPath, Codec.DEFAULT.encodeAsBytes(clusterServerInfo));
        persistentNode.start();

        Thread.sleep(1100);

        ClusterManager localClusterManager = (ClusterManager) clusterServers.getClusterServer(CLUSTER_ID);
        Assert.assertNotNull(localClusterManager);
        Assert.assertEquals(currentServer, localClusterManager);
        verify(observer, times(1)).update(any(NodeAdded.class), anyObject());

        clusterServerInfo.setPort(PORT + 1);
        doReturn(clusterServerInfo).when(currentServer).getClusterInfo();
        Thread.sleep(10);
        verify(observer, atLeast(1)).update(any(NodeModified.class), anyObject());

        Set<ClusterManager> clusterManagers = clusterServers.allClusterServers();
        Assert.assertEquals(clusterManagers.size(), 1);

        Assert.assertEquals(clusterServers.currentClusterServer(), currentServer);
        Assert.assertEquals(clusterServers.exist(CLUSTER_ID), true);

        Map<String, ClusterServerInfo> clusterServerInfoMap = clusterServers.allClusterServerInfos();
        Assert.assertEquals(clusterServerInfoMap.size(), 1);
        ClusterServerInfo serverInfo = clusterServerInfoMap.get(CLUSTER_ID);

        Assert.assertEquals(serverInfo.getPort(), clusterServerInfo.getPort());

    }

}