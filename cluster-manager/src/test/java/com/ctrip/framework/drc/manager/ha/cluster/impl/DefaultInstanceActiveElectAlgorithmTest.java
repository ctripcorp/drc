package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Author limingdong
 * @create 2020/7/7
 */
public class DefaultInstanceActiveElectAlgorithmTest extends AbstractDbClusterTest {

    private DefaultInstanceActiveElectAlgorithm<Replicator> instanceActiveElectAlgorithm = new DefaultInstanceActiveElectAlgorithm<Replicator>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        try {
            super.tearDown();
        } catch (Exception e) {
        }
    }

    @Test
    public void select() {
        List<Replicator> replicatorList = Lists.newArrayList();
        Replicator master = instanceActiveElectAlgorithm.select(CLUSTER_ID, replicatorList);
        Assert.assertNull(master);

        replicatorList.add(newReplicator);
        master = instanceActiveElectAlgorithm.select(CLUSTER_ID, replicatorList);
        Assert.assertEquals(master, newReplicator);
    }

}