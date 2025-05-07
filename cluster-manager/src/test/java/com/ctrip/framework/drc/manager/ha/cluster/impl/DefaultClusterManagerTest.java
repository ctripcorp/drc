package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterException;
import com.ctrip.framework.drc.manager.ha.cluster.SlotInfo;
import com.ctrip.framework.drc.manager.ha.cluster.SlotManager;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.ha.rest.ForwardInfo;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.zk.ZkClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Random;
import java.util.concurrent.*;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class DefaultClusterManagerTest extends AbstractDbClusterTest {

    @InjectMocks
    private DefaultClusterManager defaultClusterManager = new DefaultClusterManager();

    @Mock
    private ZkClient zkClient;

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private SlotManager slotManager;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private DcCache dcMetaCache;

    @Mock
    private ForwardInfo forwardInfo;

    @Mock
    private ClusterServerStateManager clusterServerStateManager;


    private Executor executors = Executors.newFixedThreadPool(1);

    private String server_id = String.valueOf(SERVER_ID) + new Random().nextInt();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        defaultClusterManager.setExecutors(executors);
        when(config.getClusterServerId()).thenReturn(String.valueOf(server_id));
        when(config.getClusterServerIp()).thenReturn(String.valueOf(IP));
        when(config.getClusterServerPort()).thenReturn(PORT);
        when(zkClient.get()).thenReturn(curatorFramework);

        defaultClusterManager.initialize();
        defaultClusterManager.start();
    }

    @After
    public void tearDown() {
        try {
            super.tearDown();
            defaultClusterManager.stop();
            defaultClusterManager.dispose();
        } catch (Exception e) {
        }
    }

    // normal state
    @Test
    public void addAndDeleteSlot() throws ClusterException, InterruptedException, ExecutionException, TimeoutException {
        int mockSlot = 1;
        doNothing().when(slotManager).refresh(mockSlot);
        SlotInfo slotInfo = new SlotInfo(server_id);
        when(slotManager.getSlotInfo(mockSlot)).thenReturn(slotInfo);
        CommandFuture commandFuture = defaultClusterManager.addSlot(mockSlot);
        commandFuture.get(300, TimeUnit.MILLISECONDS);
        Assert.assertEquals(commandFuture.isSuccess(), true);
        verify(currentMetaManager, times(1)).addSlot(mockSlot);

        slotInfo = new SlotInfo(server_id + 1);
        when(slotManager.getSlotInfo(mockSlot)).thenReturn(slotInfo);
        commandFuture = defaultClusterManager.deleteSlot(mockSlot);
        commandFuture.get(300, TimeUnit.MILLISECONDS);
        Assert.assertEquals(commandFuture.isSuccess(), true);
        verify(currentMetaManager, times(1)).deleteSlot(mockSlot);
    }

    // moving state
    @Test
    public void importAndExportSlot() throws ClusterException, InterruptedException, ExecutionException, TimeoutException {
        int mockSlot = 1;
        doNothing().when(slotManager).refresh(mockSlot);
        SlotInfo slotInfo = new SlotInfo(server_id + 1);
        slotInfo.moveingSlot(server_id);
        when(slotManager.getSlotInfo(mockSlot)).thenReturn(slotInfo);
        CommandFuture commandFuture = defaultClusterManager.importSlot(mockSlot);
        commandFuture.get(300, TimeUnit.MILLISECONDS);
        Assert.assertEquals(commandFuture.isSuccess(), true);
        verify(currentMetaManager, times(1)).importSlot(mockSlot);

        slotInfo = new SlotInfo(server_id);
        slotInfo.moveingSlot(server_id + 1);
        when(slotManager.getSlotInfo(mockSlot)).thenReturn(slotInfo);
        commandFuture = defaultClusterManager.exportSlot(mockSlot);
        commandFuture.get(300, TimeUnit.MILLISECONDS);
        Assert.assertEquals(commandFuture.isSuccess(), true);
        verify(currentMetaManager, times(1)).exportSlot(mockSlot);
    }

    @Test
    public void notifySlotChange() throws ClusterException, InterruptedException {
        int mockSlot = 1;
        defaultClusterManager.notifySlotChange(mockSlot);
        Thread.sleep(100);
        verify(slotManager, times(1)).refresh(mockSlot);
    }

//    @Test
    public void clusterInterface() {
        defaultClusterManager.clusterAdded("test_dc_id", CLUSTER_ID, forwardInfo, "test");
        defaultClusterManager.clusterDeleted(CLUSTER_ID, forwardInfo, "test");
        defaultClusterManager.clusterModified(CLUSTER_ID, forwardInfo, "test");
        verify(dcMetaCache, times(1)).clusterAdded(CLUSTER_ID);
        verify(dcMetaCache, times(1)).clusterDeleted(CLUSTER_ID);
        verify(dcMetaCache, times(1)).clusterModified(CLUSTER_ID);
    }

}
