package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.CurrentClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.task.ReshardingTask;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.manager.ha.cluster.impl.ArrangeTaskExecutor.ARRANGE_TASK_EXECUTOR_START;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class ArrangeTaskExecutorTest extends AbstractDbClusterTest {

    @InjectMocks
    private ArrangeTaskExecutor arrangeTaskExecutor = new ArrangeTaskExecutor();

    @Mock
    private CurrentClusterServer currentClusterServer;

    @Mock
    private ReshardingTask reshardingTask;

    @Mock
    private CommandFuture commandFuture;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(currentClusterServer.getServerId()).thenReturn(CLUSTER_ID);
        when(reshardingTask.execute()).thenReturn(commandFuture);
        when(commandFuture.await(60000, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(commandFuture.isSuccess()).thenReturn(true);
        arrangeTaskExecutor.initialize();
        arrangeTaskExecutor.start();
    }

    @After
    public void tearDown() {
        super.tearDown();
        try {
            arrangeTaskExecutor.stop();
            arrangeTaskExecutor.dispose();
        } catch (Exception e) {
        }
    }

    @Test
    public void offer() throws InterruptedException {
        System.setProperty(ARRANGE_TASK_EXECUTOR_START, "true");
        arrangeTaskExecutor.offer(reshardingTask);
        Thread.sleep(20);
        long taskCount = arrangeTaskExecutor.getTotalTasks();
        Assert.assertEquals(taskCount, 1);
        verify(reshardingTask, times(1)).execute();
    }
}