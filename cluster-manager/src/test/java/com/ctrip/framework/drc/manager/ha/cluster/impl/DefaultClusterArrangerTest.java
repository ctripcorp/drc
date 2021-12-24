package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.*;
import com.ctrip.framework.drc.manager.ha.cluster.task.ContinueResharding;
import com.ctrip.framework.drc.manager.ha.cluster.task.InitResharding;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.observer.NodeModified;
import com.ctrip.xpipe.zk.ZkClient;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class DefaultClusterArrangerTest extends AbstractDbClusterTest {

    @InjectMocks
    private DefaultClusterArranger clusterArranger = new DefaultClusterArranger();

    @Mock
    private ClusterServers<?> clusterServers;

    @Mock
    private ArrangeTaskTrigger arrangeTaskTrigger;

    @Mock
    private RemoteClusterServerFactory<ClusterServer> remoteClusterServerFactory;

    @Mock
    private SlotManager slotManager;

    @Mock
    private ClusterServer clusterManager;

    @Mock
    private Observable observable;

    @Mock
    private ZkClient zkClient;

    @Mock
    private ScheduledExecutorService scheduled;

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private Observer observer;

    private Set<Integer> SLOTS = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testInitSlot() throws ClusterException, InterruptedException {
        doNothing().when(slotManager).refresh();
        doNothing().when(clusterServers).refresh();
        when(slotManager.allSlots()).thenReturn(SLOTS);
        when(slotManager.allMoveingSlots()).thenReturn(new HashMap<>());
        when(slotManager.allServers()).thenReturn(new HashSet<>());
        clusterArranger.isleader();

        Thread.sleep(10);
        verify(arrangeTaskTrigger, times(1)).initSharding(any(InitResharding.class));
    }

    @Test
    public void testMovingSlots() throws ClusterException, InterruptedException {
        doNothing().when(slotManager).refresh();
        doNothing().when(clusterServers).refresh();
        when(slotManager.allSlots()).thenReturn(SLOTS);

        int mockSlot = 1;
        SlotInfo slotInfo = new SlotInfo(String.valueOf(SERVER_ID));
        Map<Integer, SlotInfo> movingSlots = Maps.newConcurrentMap();
        movingSlots.put(mockSlot, slotInfo);
        when(slotManager.allMoveingSlots()).thenReturn(movingSlots);

        when(slotManager.allServers()).thenReturn(new HashSet<>());
        clusterArranger.isleader();

        Thread.sleep(10);

        verify(arrangeTaskTrigger, times(1)).initSharding(any(ContinueResharding.class));
    }

    @Test
    public void testDeadServer() throws ClusterException, InterruptedException {
        doNothing().when(slotManager).refresh();
        doNothing().when(clusterServers).refresh();
        when(slotManager.allSlots()).thenReturn(SLOTS);
        when(slotManager.allMoveingSlots()).thenReturn(new HashMap<>());

        Set<String> servers = Sets.newHashSet();
        servers.add(String.valueOf(SERVER_ID));
        when(slotManager.allServers()).thenReturn(servers);
        when(clusterServers.exist(String.valueOf(SERVER_ID))).thenReturn(false);
        when(remoteClusterServerFactory.createClusterServer(String.valueOf(SERVER_ID), null)).thenReturn(clusterManager);
        clusterArranger.isleader();

        Thread.sleep(10);

        verify(arrangeTaskTrigger, times(1)).serverDead(clusterManager);
    }

    @Test
    public void update() throws ClusterException {
        doNothing().when(slotManager).refresh();
        doNothing().when(clusterServers).refresh();
        when(slotManager.allSlots()).thenReturn(SLOTS);
        when(slotManager.allMoveingSlots()).thenReturn(new HashMap<>());
        when(slotManager.allServers()).thenReturn(new HashSet<>());
        clusterArranger.isleader();

        clusterArranger.update(new NodeAdded<>(clusterManager), observable);
        verify(arrangeTaskTrigger, times(1)).serverAlive(clusterManager);

        clusterArranger.update(new NodeDeleted<>(clusterManager), observable);
        verify(arrangeTaskTrigger, times(1)).serverDead(clusterManager);

        clusterArranger.update(new NodeModified<>(null, clusterManager), observable);
    }

}