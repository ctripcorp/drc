package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServers;
import com.ctrip.framework.drc.manager.ha.cluster.SlotManager;
import com.ctrip.framework.drc.manager.ha.cluster.task.ReshardingTask;
import com.ctrip.framework.drc.manager.ha.cluster.task.ServerBalanceResharding;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.zk.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class ArrangeTaskTriggerTest extends AbstractDbClusterTest {

    @InjectMocks
    private ArrangeTaskTrigger arrangeTaskTrigger = new ArrangeTaskTrigger();

    @Mock
    private SlotManager slotManager;

    @Mock
    private ZkClient zkClient;

    @Mock
    private ClusterServers<?> clusterServers;

    @Mock
    private ArrangeTaskExecutor arrangeTaskExecutor;

    @Mock
    private ReshardingTask reshardingTask;

    @Mock
    private ClusterServer clusterServer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(zkClient.get()).thenReturn(curatorFramework);
        arrangeTaskTrigger.setScheduled(scheduledExecutorService);

        arrangeTaskTrigger.initialize();
        arrangeTaskTrigger.start();
    }

    @After
    public void tearDown() {
        try {
            super.tearDown();
            arrangeTaskTrigger.stop();
            arrangeTaskTrigger.dispose();
        } catch (Exception e) {
        }
    }

    @Test
    public void testAliveAndDead() {
        arrangeTaskTrigger.serverAlive(clusterServer);
        verify(arrangeTaskExecutor, times(1)).offer(any(ServerBalanceResharding.class));
        arrangeTaskTrigger.serverDead(clusterServer);
        arrangeTaskTrigger.serverAlive(clusterServer);  //from dead, so not reshard, may network is bad
        verify(arrangeTaskExecutor, times(1)).offer(any(ServerBalanceResharding.class));
    }
}