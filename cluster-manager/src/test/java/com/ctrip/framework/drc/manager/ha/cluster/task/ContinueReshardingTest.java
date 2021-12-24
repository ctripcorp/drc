package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.*;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class ContinueReshardingTest extends AbstractSlotTest {

    private ContinueResharding continueResharding;

    @Mock
    private RemoteClusterServerFactory<ClusterServer> remoteClusterServerFactory;

    private Map<Integer, SlotInfo> movingSlots = Maps.newConcurrentMap();

    private int mockSlot = 1;

    private SlotInfo slotInfo;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        slotInfo = new SlotInfo(String.valueOf(SERVER_ID));
        slotInfo.moveingSlot(String.valueOf(SERVER_ID) + 1);
        movingSlots.put(mockSlot, slotInfo);
        when(remoteClusterServerFactory.createClusterServer(anyString(), anyObject())).thenReturn(clusterServer);
        when(servers.getClusterServer(String.valueOf(SERVER_ID) + 1)).thenReturn(clusterServer);

        Set<ClusterServer> clusterServers = Sets.newHashSet();
        clusterServers.add(clusterServer);

        when(servers.allClusterServers()).thenReturn(clusterServers);
    }

    @Test
    public void execute() throws InterruptedException, ExecutionException, TimeoutException {
        continueResharding = new ContinueResharding(slotManager, movingSlots, servers, remoteClusterServerFactory, zkClient);
        CommandFuture commandFuture = continueResharding.execute();
        commandFuture.get(5000, TimeUnit.MILLISECONDS);
        Thread.sleep(300);
        verify(clusterServer, times(1)).notifySlotChange(mockSlot);  // local server update memory
    }
}