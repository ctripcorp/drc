package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;
import com.ctrip.framework.drc.manager.ha.cluster.META_SERVER_SERVICE;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerService;
import com.ctrip.framework.drc.manager.ha.rest.ForwardInfo;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.rest.ForwardType;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestOperations;

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
        remoteClusterManager.clusterAdded("test_dc_id", CLUSTER_ID, forwardInfo, "test");
    }

    @Test
    public void clusterModify() {
//        int maxConnPerRoute = Integer.parseInt(System.getProperty("remoteMaxConnPerRoute", "1000"));
//        int maxConnTotal = Integer.parseInt(System.getProperty("maxConnTotal", "10000"));
//        int connectTimeout = Integer.parseInt(System.getProperty("remoteConnectTimeout", "1000"));
//        int soTimeout = Integer.parseInt(System.getProperty("remoteSoTimeout", "500000"));
//        String changeClusterPath;
//        changeClusterPath = META_SERVER_SERVICE.CLUSTER_CHANGE.getRealPath("127.0.0.1:8080");
//        RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(maxConnPerRoute, maxConnTotal, connectTimeout, soTimeout);
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
//        headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON_UTF8));
//
//        String key = RegistryKey.from("bbzdrcbenchmarkdb_dalcluster", "fat-fx-drc2");
//
//        HttpEntity<DbCluster> entity = new HttpEntity<>(null, headers);
//        try {
//            restTemplate.exchange(changeClusterPath +  "?operator=wjx", HttpMethod.PUT, entity, String.class, key);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        remoteClusterManager.clusterModified(CLUSTER_ID, forwardInfo, "test");
    }

    @Test
    public void clusterDelete() {
        remoteClusterManager.clusterDeleted(CLUSTER_ID, forwardInfo, "test");
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
