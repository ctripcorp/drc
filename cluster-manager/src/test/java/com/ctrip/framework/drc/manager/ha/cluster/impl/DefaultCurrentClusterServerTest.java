package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.framework.drc.manager.zookeeper.AbstractZkTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.zk.ZkClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public class DefaultCurrentClusterServerTest extends AbstractZkTest {

    @InjectMocks
    private DefaultCurrentClusterServer defaultCurrentClusterServer;

    @Mock
    private ZkClient zkClient;
    @Mock
    private ClusterManagerConfig config;
    @Mock
    private ClusterServerStateManager clusterServerStateManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        when(zkClient.get()).thenReturn(curatorFramework);
        when(config.getClusterServerId()).thenReturn(String.valueOf("server_id_test"));
        when(config.getClusterServerIp()).thenReturn(String.valueOf(IP));
        when(config.getClusterServerPort()).thenReturn(PORT);

        defaultCurrentClusterServer.initialize();
        defaultCurrentClusterServer.start();
    }

    @Test
    public void testUpdateClusterState() throws Exception {
        Thread.sleep(100);
        String path = ClusterZkConfig.getClusterManagerRegisterPath() + "/" + defaultCurrentClusterServer.getServerId();

        ClusterServerInfo decode = Codec.DEFAULT.decode(curatorFramework.getData().forPath(path), ClusterServerInfo.class);
        Assert.assertEquals(ServerStateEnum.NORMAL, decode.getStateEnum());
        Assert.assertEquals(ServerStateEnum.NORMAL, defaultCurrentClusterServer.getClusterInfo().getStateEnum());

        // update zk
        defaultCurrentClusterServer.updateClusterState(ServerStateEnum.LOST);
        decode = Codec.DEFAULT.decode(curatorFramework.getData().forPath(path), ClusterServerInfo.class);
        Assert.assertEquals(ServerStateEnum.LOST, decode.getStateEnum());
        Assert.assertEquals(ServerStateEnum.LOST, defaultCurrentClusterServer.getClusterInfo().getStateEnum());

    }

}