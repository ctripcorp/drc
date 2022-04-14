package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import org.assertj.core.util.Lists;
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

    @Test
    public void testMulti() {
        String clusterId = "integration-test.fxdrc_multi";
        currentMeta.addCluster(dbCluster);
        Applier applier1 = new Applier();
        String name1 = "integration-test1";
        String mhaName1 = "mockTargetMha";
        applier1.setIp("127.0.0.1");
        applier1.setPort(8080);
        applier1.setTargetName(name1);
        applier1.setTargetMhaName(mhaName1);
        applier1.setMaster(true);

        Applier applier2 = new Applier();
        String name2 = "integration-test2";
        String mhaName2 = "fxdrc_multi";
        applier2.setIp("127.0.0.3");
        applier2.setPort(8080);
        applier2.setTargetName(name2);
        applier2.setTargetMhaName(mhaName2);
        applier2.setMaster(true);

        Applier applier3 = new Applier();
        String name3 = "integration-test3";
        String mhaName3 = "mockTargetMha";
        applier3.setIp("127.0.0.4");
        applier3.setPort(8080);
        applier3.setTargetName(name3);
        applier3.setTargetMhaName(mhaName3);
        applier3.setMaster(true);


        currentMeta.setSurviveAppliers(clusterId, Lists.newArrayList(applier1), applier1);
        currentMeta.setSurviveAppliers(clusterId, Lists.newArrayList(applier2), applier2);
        currentMeta.setSurviveAppliers(clusterId, Lists.newArrayList(applier3), applier3);

        Applier applier = currentMeta.getActiveApplier(clusterId, RegistryKey.from(name1, mhaName1));
        Assert.assertEquals(applier, applier1);
        currentMeta.getActiveApplier(clusterId, RegistryKey.from(name2, mhaName2));
        Assert.assertEquals(applier, applier2);
        currentMeta.getActiveApplier(clusterId, RegistryKey.from(name3, mhaName3));
        Assert.assertEquals(applier, applier3);

    }
}