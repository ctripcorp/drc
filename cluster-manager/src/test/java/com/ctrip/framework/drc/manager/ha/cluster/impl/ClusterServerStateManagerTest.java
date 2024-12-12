package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.impl.MetaRefreshDone;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

/**
 * @author yongnian
 * @create 2024/11/4 16:52
 */
public class ClusterServerStateManagerTest {
    @Mock
    private ZkClient zkClient;
    @Mock
    protected ClusterManagerConfig config;
    @Mock
    protected RegionCache regionCache;
    @Mock
    protected CuratorFramework curatorFramework;
    @Mock
    DefaultCurrentClusterServer currentClusterServer;

    @InjectMocks
    ClusterServerStateManager stateManager;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(zkClient.get()).thenReturn(curatorFramework);
        Listenable<ConnectionStateListener> mock = mock(Listenable.class);
        when(curatorFramework.getConnectionStateListenable()).thenReturn(mock);

        stateManager.initialize();
        stateManager.start();
        stateManager.addObserver(currentClusterServer);

    }

    @Test
    public void test() throws InterruptedException {
        Assert.assertEquals(ServerStateEnum.NORMAL, stateManager.getServerState());
        // alive -> lost
        stateManager.stateChanged(zkClient.get(), ConnectionState.SUSPENDED);
        stateManager.stateChanged(zkClient.get(), ConnectionState.LOST);
        Assert.assertEquals(ServerStateEnum.LOST, stateManager.getServerState());
        Thread.sleep(100);
        verify(currentClusterServer, times(1)).update(eq(ServerStateEnum.LOST), any());

        // lost -> restarting
        stateManager.stateChanged(zkClient.get(), ConnectionState.RECONNECTED);
        Assert.assertEquals(ServerStateEnum.RESTARTING, stateManager.getServerState());
        Thread.sleep(100);
        verify(currentClusterServer, times(1)).update(eq(ServerStateEnum.RESTARTING), any());


        // restarting -> alive
        stateManager.update(MetaRefreshDone.getInstance(), regionCache);
        Assert.assertEquals(ServerStateEnum.NORMAL, stateManager.getServerState());
        Thread.sleep(100);
        verify(currentClusterServer, times(1)).update(eq(ServerStateEnum.NORMAL), any());


    }
}
