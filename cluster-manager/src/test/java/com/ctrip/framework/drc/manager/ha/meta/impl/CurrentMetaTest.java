package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void testSetSurvivedMessenger() {
        currentMeta.addCluster(dbCluster);
        Messenger messenger1 = new Messenger().setIp("127.0.0.1").setMaster(true).setPort(8080).setApplyMode(ApplyMode.mq.getType());
        currentMeta.setSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerRegisterKey(CLUSTER_ID, messenger1), Lists.newArrayList(messenger1), messenger1);


        Messenger messenger2 = new Messenger().setIp("127.0.0.2").setMaster(true).setPort(8080).setApplyMode(ApplyMode.db_mq.getType()).setIncludedDbs("db2");
        Messenger messenger2Slave = new Messenger().setIp("127.0.0.3").setMaster(true).setPort(8080).setApplyMode(ApplyMode.db_mq.getType()).setIncludedDbs("db2");
        currentMeta.setSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerRegisterKey(CLUSTER_ID, messenger2), Lists.newArrayList(messenger2, messenger2Slave), messenger2);

        Messenger messenger3 = new Messenger().setIp("127.0.0.4").setMaster(true).setPort(8080).setApplyMode(ApplyMode.db_mq.getType()).setIncludedDbs("db3");
        currentMeta.setSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerRegisterKey(CLUSTER_ID, messenger3), Lists.newArrayList(messenger3), messenger3);

        List<Messenger> messengers = currentMeta.getSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerDbName(messenger1));

        Assert.assertEquals(1, messengers.size());
        Assert.assertEquals(messengers.get(0), messenger1);

        messengers = currentMeta.getSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerDbName(messenger2));
        Assert.assertEquals(2, messengers.size());
        Assert.assertEquals(messengers.get(0), messenger2);


        messengers = currentMeta.getSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerDbName(messenger3));
        Assert.assertEquals(1, messengers.size());
        Assert.assertEquals(messengers.get(0), messenger3);


        // remove
        currentMeta.setSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerRegisterKey(CLUSTER_ID, messenger3), Lists.newArrayList(), null);

        messengers = currentMeta.getSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerDbName(messenger1));

        Assert.assertEquals(1, messengers.size());
        Assert.assertEquals(messengers.get(0), messenger1);

        messengers = currentMeta.getSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerDbName(messenger2));
        Assert.assertEquals(2, messengers.size());
        Assert.assertEquals(messengers.get(0), messenger2);


        messengers = currentMeta.getSurviveMessengers(CLUSTER_ID, NameUtils.getMessengerDbName(messenger3));
        Assert.assertEquals(0, messengers.size());
    }

    @Test
    public void testMulti() {
        currentMeta.addCluster(dbCluster);
        Applier applier1 = new Applier();
        String name1 = "integration-test1";
        String mhaName1 = "mockTargetMha";
        applier1.setIp("127.0.0.1");
        applier1.setPort(8080);
        applier1.setTargetName(name1);
        applier1.setTargetMhaName(mhaName1);
        applier1.setMaster(true);
        applier1.setApplyMode(ApplyMode.transaction_table.getType());

        Applier applier1_slave = new Applier();
        applier1_slave.setIp("127.0.0.10");
        applier1_slave.setPort(8080);
        applier1_slave.setTargetName(name1);
        applier1_slave.setTargetMhaName(mhaName1);
        applier1_slave.setMaster(false);
        applier1_slave.setApplyMode(ApplyMode.transaction_table.getType());


        Applier applier2 = new Applier();
        String name2 = "integration-test2";
        String mhaName2 = "fxdrc_multi";
        applier2.setIp("127.0.0.3");
        applier2.setPort(8080);
        applier2.setTargetName(name2);
        applier2.setTargetMhaName(mhaName2);
        applier2.setMaster(true);
        applier2.setApplyMode(ApplyMode.transaction_table.getType());

        Applier applier2_slave = new Applier();
        applier2_slave.setIp("127.0.0.30");
        applier2_slave.setPort(8080);
        applier2_slave.setTargetName(name2);
        applier2_slave.setTargetMhaName(mhaName2);
        applier2_slave.setMaster(false);
        applier2_slave.setApplyMode(ApplyMode.transaction_table.getType());

        Applier applier3 = new Applier();
        String name3 = "integration-test3";
        String mhaName3 = "mockTargetMha3";
        applier3.setIp("127.0.0.4");
        applier3.setPort(8080);
        applier3.setTargetName(name3);
        applier3.setTargetMhaName(mhaName3);
        applier3.setMaster(true);
        applier3.setApplyMode(ApplyMode.transaction_table.getType());

        Applier applier4ToRemove = new Applier();
        String name4 = "integration-test4";
        String mhaName4 = "mockTargetMha4";
        applier4ToRemove.setIp("127.0.0.5");
        applier4ToRemove.setPort(8080);
        applier4ToRemove.setTargetName(name4);
        applier4ToRemove.setTargetMhaName(mhaName4);
        applier4ToRemove.setMaster(true);
        applier4ToRemove.setApplyMode(ApplyMode.transaction_table.getType());

        Applier applier5 = new Applier();
        String name5 = "integration-test5";
        String mhaName5 = "mockTargetMha5";
        applier5.setIp("127.0.0.6");
        applier5.setPort(8080);
        applier5.setTargetName(name5);
        applier5.setTargetMhaName(mhaName5);
        applier5.setIncludedDbs("db5");
        applier5.setMaster(true);
        applier5.setApplyMode(ApplyMode.db_transaction_table.getType());


        currentMeta.setSurviveAppliers(CLUSTER_ID, NameUtils.getApplierRegisterKey(CLUSTER_ID, applier1), Lists.newArrayList(applier1, applier1_slave), applier1);
        currentMeta.setSurviveAppliers(CLUSTER_ID, NameUtils.getApplierRegisterKey(CLUSTER_ID, applier2), Lists.newArrayList(applier2, applier2_slave), applier2);
        currentMeta.setSurviveAppliers(CLUSTER_ID, NameUtils.getApplierRegisterKey(CLUSTER_ID, applier3), Lists.newArrayList(applier3), applier3);
        currentMeta.setSurviveAppliers(CLUSTER_ID, NameUtils.getApplierRegisterKey(CLUSTER_ID, applier4ToRemove), Lists.newArrayList(applier4ToRemove), applier4ToRemove);
        currentMeta.setSurviveAppliers(CLUSTER_ID, NameUtils.getApplierRegisterKey(CLUSTER_ID, applier5), Lists.newArrayList(applier5), applier5);

        Applier applier = currentMeta.getActiveApplier(CLUSTER_ID, RegistryKey.from(name1, mhaName1));
        Assert.assertEquals(applier, applier1);
        applier = currentMeta.getActiveApplier(CLUSTER_ID, RegistryKey.from(name2, mhaName2));
        Assert.assertEquals(applier, applier2);
        applier = currentMeta.getActiveApplier(CLUSTER_ID, RegistryKey.from(name3, mhaName3));
        Assert.assertEquals(applier, applier3);
        applier = currentMeta.getActiveApplier(CLUSTER_ID, RegistryKey.from(name4, mhaName4));
        Assert.assertEquals(applier, applier4ToRemove);

        List<Applier> appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, RegistryKey.from(name1, mhaName1));
        Assert.assertEquals(2, appliers.size());
        Assert.assertEquals(appliers.get(0), applier1);
        Assert.assertEquals(appliers.get(1), applier1_slave);

        appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, RegistryKey.from(name2, mhaName2));
        Assert.assertEquals(2, appliers.size());
        Assert.assertEquals(appliers.get(0), applier2);
        Assert.assertEquals(appliers.get(1), applier2_slave);

        appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, RegistryKey.from(name3, mhaName3));
        Assert.assertEquals(1, appliers.size());
        Assert.assertEquals(appliers.get(0), applier3);

        appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, RegistryKey.from(name4, mhaName4));
        Assert.assertEquals(1, appliers.size());
        Assert.assertEquals(appliers.get(0), applier4ToRemove);

        appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, NameUtils.getApplierBackupRegisterKey(applier5));
        Assert.assertEquals(1, appliers.size());
        Assert.assertEquals(appliers.get(0), applier5);

        // remove
        currentMeta.setSurviveAppliers(CLUSTER_ID, NameUtils.getApplierRegisterKey(CLUSTER_ID, applier4ToRemove), Lists.newArrayList(), null);

        appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, RegistryKey.from(name1, mhaName1));
        Assert.assertEquals(2, appliers.size());
        Assert.assertEquals(appliers.get(0), applier1);
        Assert.assertEquals(appliers.get(1), applier1_slave);

        appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, RegistryKey.from(name2, mhaName2));
        Assert.assertEquals(2, appliers.size());
        Assert.assertEquals(appliers.get(0), applier2);
        Assert.assertEquals(appliers.get(1), applier2_slave);

        appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, RegistryKey.from(name3, mhaName3));
        Assert.assertEquals(1, appliers.size());
        Assert.assertEquals(appliers.get(0), applier3);

        appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, RegistryKey.from(name4, mhaName4));
        Assert.assertEquals(0, appliers.size());


        appliers = currentMeta.getSurviveAppliers(CLUSTER_ID, NameUtils.getApplierBackupRegisterKey(applier5));
        Assert.assertEquals(1, appliers.size());
    }

    @Test(timeout = 10000)
    public void testAddResource() throws Exception {
        CurrentMeta.CurrentClusterMeta currentClusterMeta = new CurrentMeta.CurrentClusterMeta();
        int testCount = 10000;
        AtomicInteger unreleasedCount = new AtomicInteger(testCount);

        List<Thread> threads = Lists.newArrayList();
        for (int i = 0; i < testCount; i++) {
            Thread thread = new Thread(() -> currentClusterMeta.addResource(unreleasedCount::decrementAndGet));
            threads.add(thread);
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        Assert.assertEquals(testCount, unreleasedCount.get());
        currentClusterMeta.release();
        Assert.assertEquals(0, unreleasedCount.get());
    }
}