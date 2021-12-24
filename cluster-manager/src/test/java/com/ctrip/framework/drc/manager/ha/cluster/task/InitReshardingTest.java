package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class InitReshardingTest extends AbstractSlotTest {

    private InitResharding initResharding;

    private Set<Integer> emptySlots = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Set<ClusterServer> clusterServers = Sets.newHashSet();
        clusterServers.add(clusterServer);

        when(servers.allClusterServers()).thenReturn(clusterServers);
        when(clusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID));
        when(slotManager.getSlotsSizeByServerId(String.valueOf(SERVER_ID))).thenReturn(0);
        when(slotManager.getSlotsSizeByServerId(String.valueOf(SERVER_ID))).thenReturn(0);
    }

    @Test
    public void testExecute() throws Exception {
        int size = 256;
        for (int i = 0; i < size; ++i) {
            emptySlots.add(i);
        }
        initResharding = new InitResharding(slotManager, emptySlots, servers, zkClient);
        CommandFuture commandFuture = initResharding.execute();
        commandFuture.get(5000, TimeUnit.MILLISECONDS);
        verify(clusterServer, times(size)).addSlot(anyInt());  // to server add slot
        verify(clusterServer, times(size)).notifySlotChange(anyInt());  // local server update memory

        for (int i = 0; i < size; ++i) {
            String path = String.format("%s/%d", ClusterZkConfig.getClusterManagerSlotsPath(), i);
            Assert.assertNotNull(curatorFramework.checkExists().forPath(path));
        }
    }

}