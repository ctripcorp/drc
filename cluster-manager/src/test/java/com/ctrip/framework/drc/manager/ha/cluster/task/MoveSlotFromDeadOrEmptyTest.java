package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.xpipe.api.command.CommandFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class MoveSlotFromDeadOrEmptyTest extends AbstractSlotTest {

    private MoveSlotFromDeadOrEmpty moveSlotFromDeadOrEmpty;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        moveSlotFromDeadOrEmpty = new MoveSlotFromDeadOrEmpty(mockSlot, clusterServer, zkClient);
    }

    @After
    public void tearDown(){
        super.tearDown();
    }

    @Test
    public void execute() throws Exception {
        CommandFuture commandFuture = moveSlotFromDeadOrEmpty.execute();
        commandFuture.get(5000, TimeUnit.MILLISECONDS);
        verify(clusterServer, times(1)).addSlot(anyInt());  // to server add slot
        String path = String.format("%s/%d", ClusterZkConfig.getClusterManagerSlotsPath(), mockSlot);
        Assert.assertNotNull(curatorFramework.checkExists().forPath(path));

    }
}