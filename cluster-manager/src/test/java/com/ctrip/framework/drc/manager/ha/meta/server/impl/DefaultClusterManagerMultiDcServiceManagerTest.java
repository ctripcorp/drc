package com.ctrip.framework.drc.manager.ha.meta.server.impl;

import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcService;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2020/5/18
 */
public class DefaultClusterManagerMultiDcServiceManagerTest extends AbstractDbClusterTest {

    private DefaultClusterManagerMultiDcServiceManager clusterManagerMultiDcServiceManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterManagerMultiDcServiceManager = new DefaultClusterManagerMultiDcServiceManager();
    }

    @Test
    public void getOrCreate() {
        ClusterManagerMultiDcService clusterManagerMultiDcService1 = clusterManagerMultiDcServiceManager.getOrCreate(LOCAL_IP);
        ClusterManagerMultiDcService clusterManagerMultiDcService2 = clusterManagerMultiDcServiceManager.getOrCreate(LOCAL_IP);
        Assert.assertEquals(clusterManagerMultiDcService1, clusterManagerMultiDcService2);
    }
}