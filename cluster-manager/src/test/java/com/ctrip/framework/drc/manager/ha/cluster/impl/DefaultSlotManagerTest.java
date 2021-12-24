package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.*;
import com.ctrip.framework.drc.manager.ha.cluster.task.MoveSlotFromDeadOrEmpty;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.zk.ZkClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class DefaultSlotManagerTest extends AbstractDbClusterTest {

    @InjectMocks
    private SlotManager slotManager = new DefaultSlotManager();

    @Mock
    private ZkClient zkClient;

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private ClusterServer clusterServer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(zkClient.get()).thenReturn(curatorFramework);
        when(config.getSlotRefreshMilli()).thenReturn(50);
//        rmr(ClusterZkConfig.getClusterManagerSlotsPath());
//        curatorFramework.createContainers(ClusterZkConfig.getClusterManagerSlotsPath());

        slotManager.initialize();
        slotManager.start();
    }

    @After
    public void tearDown() {
        try {
            super.tearDown();
            slotManager.stop();
            slotManager.dispose();
        } catch (Exception e) {
        }
    }

    @Test
    public void testAllInterfaces() throws ClusterException, InterruptedException, ExecutionException, TimeoutException {
        Assert.assertTrue(slotManager.allSlotsInfo().isEmpty());
        Assert.assertTrue(slotManager.allSlots().isEmpty());
        Assert.assertTrue(slotManager.allServers().isEmpty());

        int mockSlot = 1;
        when(clusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID));
        when(clusterServer.addSlot(mockSlot)).thenReturn(null);

        MoveSlotFromDeadOrEmpty slotFromDeadOrEmpty = new MoveSlotFromDeadOrEmpty(mockSlot, clusterServer, zkClient);
        CommandFuture commandFuture =  slotFromDeadOrEmpty.execute();
        commandFuture.get(300, TimeUnit.MILLISECONDS);
        Assert.assertTrue(commandFuture.isSuccess());

        slotManager.refresh(mockSlot);

        Assert.assertFalse(slotManager.allSlotsInfo().isEmpty());
        Assert.assertFalse(slotManager.allSlots().isEmpty());
        Assert.assertFalse(slotManager.allServers().isEmpty());

        String serverId = slotManager.getSlotServerId(mockSlot);
        Assert.assertEquals(serverId, String.valueOf(SERVER_ID));

        SlotInfo slotInfo = slotManager.getSlotInfo(mockSlot);
        Assert.assertEquals(serverId, slotInfo.getServerId());
        Assert.assertNull(slotInfo.getToServerId());
        Assert.assertEquals(slotInfo.getSlotState(), SLOT_STATE.NORMAL);

        int slotSize = slotManager.getSlotsSizeByServerId(String.valueOf(SERVER_ID));
        Assert.assertEquals(slotSize, 1);

        Set<Integer> slotSet = slotManager.getSlotsByServerId(String.valueOf(SERVER_ID));
        Assert.assertEquals(slotSet.size(), 1);

        slotSet = slotManager.getSlotsByServerId(String.valueOf(SERVER_ID), false);
        Assert.assertEquals(slotSet.size(), 1);

        serverId = slotManager.getServerIdByKey(mockSlot);
        Assert.assertEquals(serverId, String.valueOf(SERVER_ID));

        slotInfo = slotManager.getSlotInfoByKey(mockSlot);
        Assert.assertEquals(serverId, slotInfo.getServerId());
        Assert.assertNull(slotInfo.getToServerId());
        Assert.assertEquals(slotInfo.getSlotState(), SLOT_STATE.NORMAL);

        slotManager.move(mockSlot, String.valueOf(SERVER_ID), String.valueOf(SERVER_ID + 1));
        Assert.assertEquals(slotManager.allServers().size(), 2);
        slotInfo = slotManager.getSlotInfo(mockSlot);
        Assert.assertEquals(slotInfo.getServerId(), String.valueOf(SERVER_ID + 1));
    }

}