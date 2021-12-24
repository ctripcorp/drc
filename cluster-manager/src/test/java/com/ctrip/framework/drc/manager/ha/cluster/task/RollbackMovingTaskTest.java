package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.xpipe.api.command.CommandFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class RollbackMovingTaskTest extends AbstractSlotTest {

    private RollbackMovingTask rollbackMovingTask;

    @Mock
    private ClusterServer from;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(from.getServerId()).thenReturn(String.valueOf(SERVER_ID));

        rollbackMovingTask = new RollbackMovingTask(mockSlot, from, clusterServer, zkClient);
    }

    @Test
    public void execute() throws InterruptedException, ExecutionException, TimeoutException {
        CommandFuture commandFuture = rollbackMovingTask.execute();
        commandFuture.get(5000, TimeUnit.MILLISECONDS);
        verify(clusterServer, times(1)).deleteSlot(mockSlot);
        verify(from, times(1)).addSlot(mockSlot);
    }
}