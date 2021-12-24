package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;
import com.ctrip.framework.drc.manager.ha.rest.ForwardInfo;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.rest.ForwardType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @Author limingdong
 * @create 2020/5/14
 */
public class RemoteClusterManagerTest extends AbstractDbClusterTest {

    private RemoteClusterManager remoteClusterManager;

    private String currentServerId = String.valueOf(SERVER_ID);

    private String remoteServerId = String.valueOf(SERVER_ID) + 1;

    private ClusterServerInfo clusterServerInfo;

    private ForwardInfo forwardInfo;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterServerInfo = new ClusterServerInfo();
        clusterServerInfo.setIp("127.0.0.1");
        clusterServerInfo.setPort(8080);

        forwardInfo = new ForwardInfo(ForwardType.FORWARD);
        remoteClusterManager = new RemoteClusterManager(currentServerId, remoteServerId, clusterServerInfo);
    }

    //for all not throw exceptions
    @Test
    public void clusterAdded() {
        remoteClusterManager.clusterAdded(dbCluster, forwardInfo);
    }

    @Test
    public void clusterModify() {
        remoteClusterManager.clusterModified(dbCluster, forwardInfo);
    }

    @Test
    public void clusterDelete() {
        remoteClusterManager.clusterDeleted(CLUSTER_ID, forwardInfo);
    }

    @Test
    public void addSlot() throws InterruptedException, ExecutionException, TimeoutException {
        int mockSlot = 1;
        CommandFuture commandFuture = remoteClusterManager.addSlot(mockSlot);
        commandFuture.get(100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(commandFuture.isSuccess(), true);
    }

    @Test
    public void deleteSlot() throws InterruptedException, ExecutionException, TimeoutException {
        int mockSlot = 1;
        CommandFuture commandFuture = remoteClusterManager.deleteSlot(mockSlot);
        commandFuture.get(100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(commandFuture.isSuccess(), true);
    }

    @Test
    public void importSlot() throws InterruptedException, ExecutionException, TimeoutException {
        int mockSlot = 1;
        CommandFuture commandFuture = remoteClusterManager.importSlot(mockSlot);
        commandFuture.get(100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(commandFuture.isSuccess(), true);
    }

    @Test
    public void exportSlot() throws InterruptedException, ExecutionException, TimeoutException {
        int mockSlot = 1;
        CommandFuture commandFuture = remoteClusterManager.exportSlot(mockSlot);
        commandFuture.get(100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(commandFuture.isSuccess(), true);
    }

    @Test
    public void notifySlotChange() throws InterruptedException{
        int mockSlot = 1;
        remoteClusterManager.notifySlotChange(mockSlot);
        Thread.sleep(100);  //not throw exception
    }
}