package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

/**
 * @Author limingdong
 * @create 2020/5/13
 */
public class CurrentMetaTest extends AbstractDbClusterTest {

    private CurrentMeta currentMeta = new CurrentMeta();

    @Mock
    private Releasable releasable;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWatch() {
        currentMeta.addCluster(dbCluster);
        Assert.assertTrue(currentMeta.hasCluster(CLUSTER_ID));
        Assert.assertEquals(currentMeta.allClusters().size(), 1);
        Assert.assertTrue(currentMeta.watchReplicatorIfNotWatched(CLUSTER_ID));
        Assert.assertTrue(currentMeta.watchReplicatorIfNotWatched(CLUSTER_ID + "faked"));
    }

    @Test
    public void allClusters() throws Exception {
        currentMeta.addCluster(dbCluster);
        currentMeta.addResource(CLUSTER_ID, releasable);
        List<Replicator> replicatorList = dbCluster.getReplicators();
        Replicator master = replicatorList.get(0);
        replicatorList.add(newReplicator);
        currentMeta.setSurviveReplicators(CLUSTER_ID, replicatorList, master);
        CurrentMeta.CurrentClusterMeta currentClusterMeta = currentMeta.removeCluster(CLUSTER_ID);
        List<Replicator> surviveReplicators = currentClusterMeta.getSurviveReplicators();
        Assert.assertEquals(surviveReplicators.size(), 2);
        Assert.assertEquals(master.getMaster(), false);  //copy
        for (Replicator replicator : surviveReplicators) {
            if (replicator.equalsWithIpPort(master)) {
                Assert.assertEquals(replicator.getMaster(), true);  //indeed
            }
        }
        verify(releasable, times(1)).release();
    }
}