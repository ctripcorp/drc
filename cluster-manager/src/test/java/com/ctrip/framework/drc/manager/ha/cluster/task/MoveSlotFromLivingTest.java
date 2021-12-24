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
public class MoveSlotFromLivingTest extends AbstractSlotTest {

    private MoveSlotFromLiving moveSlotFromLiving;

    @Mock
    private ClusterServer from;

    @Mock
    private CommandFuture commandFuture;

    @Mock
    private CommandFuture failCommandFuture;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(from.getServerId()).thenReturn(String.valueOf(SERVER_ID));
        when(clusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID) + 1);
        when(clusterServer.importSlot(mockSlot)).thenReturn(commandFuture);
        when(from.exportSlot(mockSlot)).thenReturn(commandFuture);
        when(commandFuture.await(10000, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(commandFuture.isSuccess()).thenReturn(true);

        moveSlotFromLiving = new MoveSlotFromLiving(mockSlot, from, clusterServer, zkClient);
    }

    @Test
    public void executeSuccess() throws InterruptedException, ExecutionException, TimeoutException {
        CommandFuture commandFuture = moveSlotFromLiving.execute();
        commandFuture.get(5000, TimeUnit.MILLISECONDS);
        verify(clusterServer, times(1)).addSlot(mockSlot);
        verify(from, times(1)).deleteSlot(mockSlot);
    }

    @Test
    public void executeFail() throws InterruptedException, ExecutionException, TimeoutException {
        when(from.exportSlot(mockSlot)).thenReturn(failCommandFuture);
        when(failCommandFuture.await(10000, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(failCommandFuture.isSuccess()).thenReturn(false);
        CommandFuture commandFuture = moveSlotFromLiving.execute();
        commandFuture.get(5000, TimeUnit.MILLISECONDS);
        verify(from, times(1)).addSlot(mockSlot);
        verify(clusterServer, times(1)).deleteSlot(mockSlot);
    }
}