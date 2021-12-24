package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServers;
import com.ctrip.framework.drc.manager.ha.cluster.SlotManager;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.zk.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public abstract class AbstractSlotTest extends AbstractDbClusterTest {

    @Mock
    protected SlotManager slotManager;

    @Mock
    protected ClusterServers servers;

    @Mock
    protected ClusterServer clusterServer;

    @Mock
    protected ZkClient zkClient;

    protected int mockSlot = 1;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        doNothing().when(slotManager).refresh();
        when(zkClient.get()).thenReturn(curatorFramework);
    }

    @After
    public void tearDown() {
        super.tearDown();
    }
}
