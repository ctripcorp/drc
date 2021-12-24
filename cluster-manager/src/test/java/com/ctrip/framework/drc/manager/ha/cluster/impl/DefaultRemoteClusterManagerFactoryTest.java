package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterManager;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class DefaultRemoteClusterManagerFactoryTest extends AbstractDbClusterTest {

    @InjectMocks
    private DefaultRemoteClusterManagerFactory remoteClusterManagerFactory = new DefaultRemoteClusterManagerFactory();

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private ClusterManager mockClusterManager;

    private ClusterServerInfo clusterServerInfo;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterServerInfo = new ClusterServerInfo();
        clusterServerInfo.setIp("127.0.0.1");
        clusterServerInfo.setPort(8080);
    }

    @Test
    public void testCreateLocal() {

        when(config.getClusterServerId()).thenReturn(String.valueOf(SERVER_ID));
        ClusterServer clusterServer = remoteClusterManagerFactory.createClusterServer(String.valueOf(SERVER_ID), clusterServerInfo);
        Assert.assertEquals(mockClusterManager, clusterServer);
    }

    @Test
    public void testCreateRemote() {

        when(config.getClusterServerId()).thenReturn(String.valueOf(SERVER_ID) + 1);
        when(mockClusterManager.getServerId()).thenReturn(String.valueOf(SERVER_ID));
        ClusterManager clusterManager = remoteClusterManagerFactory.createClusterServer(String.valueOf(SERVER_ID), clusterServerInfo);
        Assert.assertTrue(clusterManager instanceof RemoteClusterManager);
    }
}