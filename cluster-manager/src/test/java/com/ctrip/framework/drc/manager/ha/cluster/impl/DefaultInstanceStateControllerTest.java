package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.tuple.Pair;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @Author limingdong
 * @create 2020/5/19
 */
public class DefaultInstanceStateControllerTest extends AbstractDbClusterTest {

    @InjectMocks
    private DefaultInstanceStateController instanceStateController = new DefaultInstanceStateController();

    private ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("UT_DefaultInstanceStateControllerTest");

    @Mock
    private DcCache dcMetaCache;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private ClusterManagerConfig config;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        instanceStateController.initialize();
        instanceStateController.setExecutors(executorService);
        instanceStateController.start();

        when(config.getMigrationBlackIps()).thenReturn(StringUtils.EMPTY);
    }

    @After
    public void tearDown() {
        super.tearDown();
        try {
            instanceStateController.stop();
            instanceStateController.dispose();
        } catch (Exception e) {
        }
    }

    @Test
    public void registerReplicator() throws InterruptedException {
        List<Replicator> replicatorList = Lists.newArrayList();
        newReplicator.setIp(LOCAL_IP);
        newReplicator.setPort(backupPort);
        replicatorList.add(newReplicator);
        when(currentMetaManager.getSurviveReplicators(CLUSTER_ID)).thenReturn(replicatorList);

        when(dcMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

        DbCluster body = instanceStateController.registerReplicator(CLUSTER_ID, newReplicator);
        Assert.assertEquals(body.getDbs(), dbCluster.getDbs());
        Assert.assertEquals(body.getAppliers(), dbCluster.getAppliers());
        List<Replicator> replicators = body.getReplicators();
        Assert.assertEquals(replicators.size(), 1);
        Assert.assertEquals(replicators.get(0), newReplicator);
        Thread.sleep(100);
    }

    @Test
    public void addReplicator() throws InterruptedException {
        List<Replicator> replicatorList = Lists.newArrayList();
        newReplicator.setIp(LOCAL_IP);
        newReplicator.setPort(backupPort);
        replicatorList.add(newReplicator);
        when(currentMetaManager.getSurviveReplicators(CLUSTER_ID)).thenReturn(replicatorList);

        when(dcMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

        DbCluster body = instanceStateController.addReplicator(CLUSTER_ID, newReplicator);
        Assert.assertEquals(body.getDbs(), dbCluster.getDbs());
        Assert.assertEquals(body.getAppliers(), dbCluster.getAppliers());
        List<Replicator> replicators = body.getReplicators();
        Assert.assertEquals(replicators.size(), 1);
        Assert.assertEquals(replicators.get(0), newReplicator);
        Thread.sleep(100);
    }

    @Test
    public void registerApplierWithMasterNull() throws InterruptedException {
        Applier applier = new Applier();
        applier.setTargetMhaName("mockTargetMha_*&^%");
        applier.setTargetIdc("mockTargetIdc_*&^%");
        applier.setMaster(true);
        applier.setIp(LOCAL_IP);
        applier.setPort(backupPort);

        List<Replicator> replicatorList = Lists.newArrayList();
        newReplicator.setIp(LOCAL_IP);
        newReplicator.setPort(backupPort);
        replicatorList.add(newReplicator);
        when(currentMetaManager.getApplierMaster(anyString(), anyString())).thenReturn(null);
        when(dcMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

        DbCluster body = instanceStateController.registerApplier(CLUSTER_ID, applier);

        Assert.assertEquals(body.getDbs(), dbCluster.getDbs());
        Assert.assertNotEquals(body.getReplicators(), dbCluster.getReplicators());
        List<Applier> appliers = body.getAppliers();
        Assert.assertEquals(appliers.size(), 1);
        Assert.assertEquals(appliers.get(0), applier);

        Thread.sleep(100);
    }

    @Test
    public void registerApplierWithMasterNotNull() throws InterruptedException {
        Applier applier = new Applier();
        applier.setTargetMhaName("mockTargetMha_*&^%");
        applier.setTargetIdc("mockTargetIdc_*&^%");
        applier.setMaster(true);
        applier.setIp(LOCAL_IP);
        applier.setPort(backupPort);

        when(currentMetaManager.getApplierMaster(anyString(), anyString())).thenReturn(applierMaster);
        when(dcMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

        DbCluster body = instanceStateController.registerApplier(CLUSTER_ID, applier);

        Assert.assertEquals(body.getDbs(), dbCluster.getDbs());
        Assert.assertNotEquals(body.getReplicators(), dbCluster.getReplicators());
        List<Replicator> replicators = body.getReplicators();
        Assert.assertEquals(replicators.size(), 1);
        Assert.assertEquals(replicators.get(0).getIp(), newReplicator.getIp());
        Assert.assertEquals(replicators.get(0).getApplierPort(), newReplicator.getApplierPort());
        Assert.assertEquals(replicators.get(0).getMaster(), true);

        List<Applier> appliers = body.getAppliers();
        Assert.assertEquals(appliers.size(), 1);
        Assert.assertEquals(appliers.get(0), applier);

        Thread.sleep(100);
    }

    @Test
    public void addApplierWithMasterNull() throws InterruptedException {
        Applier applier = new Applier();
        applier.setTargetMhaName("mockTargetMha_*&^%");
        applier.setTargetIdc("mockTargetIdc_*&^%");
        applier.setMaster(true);
        applier.setIp(LOCAL_IP);
        applier.setPort(backupPort);

        List<Replicator> replicatorList = Lists.newArrayList();
        newReplicator.setIp(LOCAL_IP);
        newReplicator.setPort(backupPort);
        replicatorList.add(newReplicator);
        when(currentMetaManager.getApplierMaster(anyString(), anyString())).thenReturn(null);
        when(dcMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

        DbCluster body = instanceStateController.addApplier(CLUSTER_ID, applier);

        Assert.assertEquals(body.getDbs(), dbCluster.getDbs());
        Assert.assertNotEquals(body.getReplicators(), dbCluster.getReplicators());
        List<Applier> appliers = body.getAppliers();
        Assert.assertEquals(appliers.size(), 1);
        Assert.assertEquals(appliers.get(0), applier);

        Thread.sleep(100);
    }


    @Test
    public void addApplierWithMasterNotNull() throws InterruptedException {
        String cluster_multi = "integration-test.fxdrc_multi";
        Applier applier = new Applier();
        applier.setTargetMhaName("mockTargetMha");
        applier.setTargetName("integration-test");
        applier.setTargetIdc("mockTargetIdc_*&^%");
        applier.setMaster(true);
        applier.setIp(LOCAL_IP);
        applier.setPort(backupPort);

        Applier multiapplier = new Applier();
        multiapplier.setTargetMhaName("mockTargetMha_multi");
        multiapplier.setTargetIdc("mockTargetIdc_*&^%");
        multiapplier.setMaster(true);
        multiapplier.setIp("127.0.0.3");
        multiapplier.setPort(backupPort);

        Applier slaveApplier = new Applier();
        slaveApplier.setTargetMhaName("mockTargetMha");
        slaveApplier.setTargetIdc("mockTargetIdc_*&^%");
        slaveApplier.setMaster(false);
        slaveApplier.setIp("127.0.0.3");
        slaveApplier.setPort(backupPort);
        List<Applier> surviveAppliers = Lists.newArrayList(applier, slaveApplier, multiapplier);

        Pair<String, Integer> applierMaster = new Pair<>(newReplicator.getIp(), newReplicator.getApplierPort());
        when(currentMetaManager.getApplierMaster(anyString(), anyString())).thenReturn(applierMaster);
        when(dcMetaCache.getCluster(cluster_multi)).thenReturn(dbCluster);
        when(currentMetaManager.getSurviveAppliers(cluster_multi, RegistryKey.from(applier.getTargetName(), applier.getTargetMhaName()))).thenReturn(surviveAppliers);

        DbCluster body = instanceStateController.addApplier(cluster_multi, applier);

        Assert.assertEquals(body.getDbs(), dbCluster.getDbs());
        Assert.assertNotEquals(body.getReplicators(), dbCluster.getReplicators());
        List<Replicator> replicators = body.getReplicators();
        Assert.assertEquals(replicators.size(), 1);
        Assert.assertEquals(replicators.get(0).getIp(), newReplicator.getIp());
        Assert.assertEquals(replicators.get(0).getApplierPort(), newReplicator.getApplierPort());
        Assert.assertEquals(replicators.get(0).getMaster(), true);

        List<Applier> appliers = body.getAppliers();
        Assert.assertEquals(appliers.size(), 1);
        Assert.assertEquals(appliers.get(0), applier);

        Thread.sleep(100);
    }

    @Test
    public void applierMasterChange() throws InterruptedException {
        Pair<String, Integer> applierMaster = new Pair<>(newReplicator.getIp(), newReplicator.getApplierPort());
        when(currentMetaManager.getApplierMaster(anyString(), anyString())).thenReturn(applierMaster);
        when(currentMetaManager.getMySQLMaster(anyString())).thenReturn(switchedMySQLMaster);
        when(dcMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

        DbCluster body = instanceStateController.applierMasterChange(CLUSTER_ID, applierMaster, newApplier);

        Assert.assertNotEquals(body.getReplicators(), dbCluster.getReplicators());
        List<Replicator> replicators = body.getReplicators();
        Assert.assertEquals(replicators.size(), 1);
        Assert.assertEquals(replicators.get(0).getIp(), newReplicator.getIp());
        Assert.assertEquals(replicators.get(0).getApplierPort(), newReplicator.getApplierPort());
        Assert.assertEquals(replicators.get(0).getMaster(), true);

        List<Applier> appliers = body.getAppliers();
        Assert.assertEquals(appliers.size(), 1);
        Assert.assertEquals(appliers.get(0), newApplier);

        Assert.assertNotEquals(body.getDbs(), dbCluster.getDbs());
        List<Db> bodyDbs = body.getDbs().getDbs();
        for (Db db : bodyDbs) {
            if (db.isMaster()) {
                Assert.assertEquals(db.getIp(), switchedMySQLMaster.getIp());
                Assert.assertEquals(db.getPort().intValue(), switchedMySQLMaster.getPort());
            }
        }

        Thread.sleep(100);
    }

    @Test
    public void mysqlMasterChanged() {
        Applier applier = new Applier();
        applier.setTargetMhaName("mockTargetMha_*&^%");
        applier.setTargetIdc("mockTargetIdc_*&^%");
        applier.setMaster(true);
        applier.setIp(LOCAL_IP);
        applier.setPort(backupPort);
        List<Applier> appliers = Lists.newArrayList(applier);

        Pair<String, Integer> applierMaster = new Pair<>(newReplicator.getIp(), newReplicator.getApplierPort());

        when(currentMetaManager.getApplierMaster(anyString(), anyString())).thenReturn(applierMaster);
        when(dcMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

        List<DbCluster> body = instanceStateController.mysqlMasterChanged(CLUSTER_ID, mysqlMaster, appliers, newReplicator);

        Assert.assertEquals(body.size(), appliers.size() + 1);
        DbCluster applierDbCluster = body.get(0);

        Assert.assertNotEquals(applierDbCluster.getReplicators(), dbCluster.getReplicators()); //
        List<Replicator> replicators = applierDbCluster.getReplicators();
        Assert.assertEquals(replicators.size(), 1);
        Assert.assertEquals(replicators.get(0).getIp(), applierMaster.getKey());
        Assert.assertEquals(replicators.get(0).getApplierPort(), applierMaster.getValue());
        Assert.assertEquals(replicators.get(0).getMaster(), true);

        Db masterDb = applierDbCluster.getDbs().getDbs().get(0);
        Assert.assertEquals(masterDb.getIp(), mysqlMaster.getHost());
        Assert.assertEquals(masterDb.getPort().intValue(), mysqlMaster.getPort());

        DbCluster replicatorDbCluster = body.get(1);
        Assert.assertNotEquals(replicatorDbCluster.getReplicators(), dbCluster.getReplicators()); //
        replicators = replicatorDbCluster.getReplicators();
        Assert.assertEquals(replicators.size(), 1);
        Assert.assertEquals(replicators.get(0), newReplicator);

        masterDb = replicatorDbCluster.getDbs().getDbs().get(0);
        Assert.assertEquals(masterDb.getIp(), mysqlMaster.getHost());
        Assert.assertEquals(masterDb.getPort().intValue(), mysqlMaster.getPort());

    }

    @Test
    public void testRemoveApplier() throws InterruptedException {
        instanceStateController.removeApplier(CLUSTER_ID, newApplier, false);
        Thread.sleep(50);
    }
}
