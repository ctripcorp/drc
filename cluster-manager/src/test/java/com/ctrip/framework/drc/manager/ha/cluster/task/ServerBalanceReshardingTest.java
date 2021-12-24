package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class ServerBalanceReshardingTest extends AbstractSlotTest {

    private ServerBalanceResharding serverBalanceResharding;

    private Set<ClusterServer> clusterServers = Sets.newHashSet();

    @Mock
    private ClusterServer newClusterServer;

    @Mock
    private CommandFuture importCommandFuture;

    @Mock
    private CommandFuture exportCommandFuture;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(servers.allClusterServers()).thenReturn(clusterServers);

        serverBalanceResharding = new ServerBalanceResharding(slotManager, servers, zkClient);
    }

    @Test
    public void executeEmpty() {
        CommandFuture commandFuture = serverBalanceResharding.execute();
        Assert.assertTrue(commandFuture.isSuccess());
    }

    @Test
    public void executeNoEasyServer() throws InterruptedException, TimeoutException, ExecutionException {
        when(clusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID));
        when(slotManager.getSlotsSizeByServerId(String.valueOf(SERVER_ID))).thenReturn(256);
        clusterServers.add(clusterServer);

        CommandFuture commandFuture = serverBalanceResharding.execute();
        commandFuture.get(5000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(commandFuture.isSuccess());
    }

    @Test
    public void executeEasyServer() throws InterruptedException, TimeoutException, ExecutionException {
        when(clusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID));
        when(newClusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID) + 1);
        when(slotManager.getSlotsSizeByServerId(String.valueOf(SERVER_ID))).thenReturn(256);
        when(slotManager.getSlotsSizeByServerId(String.valueOf(SERVER_ID) + 1)).thenReturn(0);
        clusterServers.add(clusterServer);
        clusterServers.add(newClusterServer);

        when(servers.allClusterServers()).thenReturn(clusterServers);
        Set<Integer> slots = Sets.newHashSet();
        int maxSlot = 256;
        for (int i = 0; i < maxSlot; ++i) {
            slots.add(i);
        }
        when(slotManager.getSlotsByServerId(String.valueOf(SERVER_ID))).thenReturn(slots);
        when(clusterServer.exportSlot(anyInt())).thenReturn(exportCommandFuture);
        when(exportCommandFuture.await(10000, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(exportCommandFuture.isSuccess()).thenReturn(true);

        when(newClusterServer.importSlot(anyInt())).thenReturn(importCommandFuture);
        when(importCommandFuture.await(10000, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(importCommandFuture.isSuccess()).thenReturn(true);


        CommandFuture commandFuture = serverBalanceResharding.execute();
        commandFuture.get(5000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(commandFuture.isSuccess());
        verify(clusterServer, times(maxSlot / 2)).notifySlotChange(anyInt());  // local server update memory
    }
}