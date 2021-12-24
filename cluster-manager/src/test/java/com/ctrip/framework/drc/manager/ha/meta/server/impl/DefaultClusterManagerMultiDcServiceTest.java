package com.ctrip.framework.drc.manager.ha.meta.server.impl;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.cluster.META_SERVER_SERVICE;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.web.client.RestOperations;

import static com.ctrip.framework.drc.manager.AllTests.BACKUP_DAL_CLUSTER_ID;
import static com.ctrip.framework.drc.manager.AllTests.DAL_CLUSTER_ID;

/**
 * @Author limingdong
 * @create 2020/5/18
 */
public class DefaultClusterManagerMultiDcServiceTest extends AbstractDbClusterTest {

    private DefaultClusterManagerMultiDcService defaultClusterManagerMultiDcService;

    @Mock
    private RestOperations restTemplate;

    private String host = LOCAL_IP + ":" + backupPort;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        defaultClusterManagerMultiDcService = new DefaultClusterManagerMultiDcService(host);
    }

    @Test
    public void upstreamChange() {
        defaultClusterManagerMultiDcService.upstreamChange(CLUSTER_ID, BACKUP_DAL_CLUSTER_ID, LOCAL_IP, backupPort);
    }

    @Test
    public void getActiveReplicator() {
        newReplicator.setIp(LOCAL_IP);
        newReplicator.setPort(backupPort);
        String activeReplicatorPath = META_SERVER_SERVICE.GET_ACTIVE_REPLICATOR.getRealPath(host);
        when(restTemplate.getForObject(activeReplicatorPath, Replicator.class, DAL_CLUSTER_ID)).thenReturn(newReplicator);
        Replicator replicator = defaultClusterManagerMultiDcService.getActiveReplicator(CLUSTER_ID);
        Assert.assertNotNull(replicator);
        Assert.assertEquals(replicator.getIp(), LOCAL_IP);
        Assert.assertEquals(replicator.getPort().intValue(), backupPort);
    }
}