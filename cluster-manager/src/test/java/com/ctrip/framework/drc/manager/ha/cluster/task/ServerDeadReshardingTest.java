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
public class ServerDeadReshardingTest extends AbstractSlotTest {

    private ServerDeadResharding serverDeadResharding;

    private Set<ClusterServer> clusterServers = Sets.newHashSet();

    @Mock
    private ClusterServer newClusterServer;

    private int averageSlot = 128;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterServers.add(clusterServer);
        clusterServers.add(newClusterServer);
        when(clusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID));
        when(newClusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID) + 1);
        when(servers.allClusterServers()).thenReturn(clusterServers);

        Set<Integer> slots = Sets.newHashSet();
        for (int i = 0; i < averageSlot; ++i) {
            slots.add(i);
        }
        when(slotManager.getSlotsByServerId(String.valueOf(SERVER_ID))).thenReturn(slots);

        when(slotManager.getSlotsSizeByServerId(anyString())).thenReturn(128);

        serverDeadResharding = new ServerDeadResharding(slotManager, clusterServer, servers, zkClient);

    }

    @Test
    public void execute() throws InterruptedException, ExecutionException, TimeoutException {
        CommandFuture commandFuture = serverDeadResharding.execute();
        commandFuture.get(5000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(commandFuture.isSuccess());
        verify(clusterServer, times(averageSlot)).notifySlotChange(anyInt());  // local server update memory
    }
}