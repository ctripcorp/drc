package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.comparator.DcRouteComparator;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.StateChangeHandler;
import com.ctrip.framework.drc.manager.ha.cluster.CurrentClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.SlotManager;
import com.ctrip.framework.drc.manager.ha.cluster.impl.ClusterServerStateManager;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.DcComparator;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.framework.drc.manager.AllTests.DAL_CLUSTER_ID;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * @Author limingdong
 * @create 2020/5/13
 */
public class DefaultCurrentMetaManagerTest extends AbstractDbClusterTest {

    private static final Long SERVER_ID = 123456789L;

    private static final int ADDED_SLOT = 3;

    @InjectMocks
    private DefaultCurrentMetaManager currentMetaManager = new DefaultCurrentMetaManager();

    @Mock
    private SlotManager slotManager;

    @Mock
    private CurrentClusterServer currentClusterServer;

    @Mock
    private RegionCache regionMetaCache;

    @Mock
    private StateChangeHandler handler;

    private Executor executors = Executors.newFixedThreadPool(1);

    @Mock
    private Observer observer;

    @Mock
    private Observable observable;

    @Mock
    private ClusterServerStateManager clusterServerStateManager;

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("unit_test");

    private Set<Integer> DEFAULT_SLOT = Sets.newHashSet();

    private Set<String> DEFAULT_CLUSTER = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(currentClusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID));
        when(currentClusterServer.slots()).thenReturn(DEFAULT_SLOT);

        DEFAULT_CLUSTER.add(DAL_CLUSTER_ID);
        when(regionMetaCache.getClusters()).thenReturn(DEFAULT_CLUSTER);

        when(slotManager.getSlotIdByKey(DAL_CLUSTER_ID)).thenReturn(ADDED_SLOT);
        when(regionMetaCache.getCluster(DAL_CLUSTER_ID)).thenReturn(dbCluster);

        currentMetaManager.setScheduled(scheduledExecutorService);
        currentMetaManager.setSlotCheckInterval(50);

        currentMetaManager.initialize();
        currentMetaManager.setExecutors(executors);
        currentMetaManager.start();

        when(clusterServerStateManager.getServerState()).thenReturn(ServerStateEnum.NORMAL);
    }

    @Test
    public void addAndRemoveSlot() throws InterruptedException {
        currentMetaManager.addObserver(observer);
        Set<Integer> addedSlots = Sets.newHashSet(DEFAULT_SLOT);
        addedSlots.add(ADDED_SLOT);
        when(slotManager.getSlotsByServerId(String.valueOf(SERVER_ID), false)).thenReturn(addedSlots);

        Thread.sleep(60);
        String metsDesc = currentMetaManager.getCurrentMetaDesc();
        JsonCodec codec = new JsonCodec(true, true);
        Map<String, Object> metaMap = codec.decode(metsDesc, new GenericTypeReference<Map<String, Object>>() {});
//        CurrentMeta metaObject = (CurrentMeta) metaMap.get("meta");
        List<Integer> slots = (List<Integer>) metaMap.get("currentSlots");
        Assert.assertEquals(slots.size(), 1);
        Assert.assertTrue(slots.contains(ADDED_SLOT));
        verify(observer, times(1)).update(any(NodeAdded.class), any(Observable.class));

        Set<String> clusters = currentMetaManager.allClusters();
        Assert.assertEquals(clusters.size(), 1);
        Assert.assertEquals(clusters.contains(CLUSTER_ID), true);

        //test remove
        addedSlots.remove(ADDED_SLOT);
        when(slotManager.getSlotsByServerId(String.valueOf(SERVER_ID), false)).thenReturn(addedSlots);
        Thread.sleep(60);
        metsDesc = currentMetaManager.getCurrentMetaDesc();
        metaMap = codec.decode(metsDesc, new GenericTypeReference<Map<String, Object>>() {});
        slots = (List<Integer>) metaMap.get("currentSlots");
        Assert.assertEquals(slots.size(), 0);
        verify(observer, times(1)).update(any(NodeDeleted.class), any(Observable.class));
    }

    @Test
    public void updateAdd() throws InterruptedException {
        currentMetaManager.addObserver(observer);
        Dc future = MetaClone.clone(current);
        DbCluster dbCluster = future.getDbClusters().get(CLUSTER_ID);
        DbCluster futureDbCluster = MetaClone.clone(dbCluster);
        String newId = CLUSTER_ID + ".added";
        futureDbCluster.setId(newId);
        future.addDbCluster(futureDbCluster);

        DcComparator dcMetaComparator = new DcComparator(current, future);
        dcMetaComparator.compare();

        when(currentClusterServer.hasKey(newId)).thenReturn(true);
        when(regionMetaCache.getCluster(newId)).thenReturn(futureDbCluster);

        currentMetaManager.update(dcMetaComparator, observable);
        Thread.sleep(60);
        verify(observer, times(1)).update(any(NodeAdded.class), any(Observable.class));
    }

    @Test
    public void updateRemove() throws InterruptedException {
        currentMetaManager.addObserver(observer);
        Dc future = MetaClone.clone(current);
        DbCluster dbCluster = future.getDbClusters().remove(CLUSTER_ID);

        DcComparator dcMetaComparator = new DcComparator(current, future);
        dcMetaComparator.compare();

        when(currentClusterServer.hasKey(CLUSTER_ID)).thenReturn(true);
        when(regionMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

        currentMetaManager.update(dcMetaComparator, observable);  //due to no cluster in currentmeta
        Thread.sleep(60);
        verify(observer, times(0)).update(any(NodeDeleted.class), any(Observable.class));
    }

    @Test
    public void updateModify() throws InterruptedException {
        currentMetaManager.addObserver(observer);
        Dc future = MetaClone.clone(current);
        DbCluster dbCluster = future.getDbClusters().get(CLUSTER_ID);
        dbCluster.getReplicators().get(0).setMaster(true);

        DcComparator dcMetaComparator = new DcComparator(current, future);
        dcMetaComparator.compare();

        when(currentClusterServer.hasKey(CLUSTER_ID)).thenReturn(true);
        when(regionMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

        currentMetaManager.update(dcMetaComparator, observable);  //due to no cluster in currentmeta
        Thread.sleep(60);
        verify(observer, times(1)).update(any(NodeAdded.class), any(Observable.class));
    }

    @Test
    public void testRouteChangeCalledAsObserver() {
        currentMetaManager = spy(currentMetaManager);

        currentMetaManager.update(new DcRouteComparator(null, null), null);
        verify(currentMetaManager, never()).dcMetaChange(Mockito.any());

        verify(currentMetaManager, times(1)).routeChanges();
    }

    @Test
    public void testRefreshApplierMaster() {
        currentMetaManager = spy(new DefaultCurrentMetaManager());
        currentMetaManager.setRegionCache(regionMetaCache);
        String clusterId = CLUSTER_ID;

        when(currentMetaManager.allClusters()).thenReturn(Sets.newHashSet(clusterId));
        Assert.assertFalse(currentMetaManager.allClusters().isEmpty());

        doReturn(Pair.from("127.0.0.1", randomPort())).when(currentMetaManager).getApplierMaster(anyString(), anyString());

        when(regionMetaCache.randomRoute(clusterId, "sharb")).thenReturn(new Route(1000).setTag(Route.TAG_META));
        when(regionMetaCache.getCluster(clusterId)).thenReturn(getCluster("shaoy", clusterId));

        currentMetaManager.addStateChangeHandler(handler);
        currentMetaManager.routeChanges();

        verify(handler, times(1)).applierMasterChanged(eq(clusterId), anyString(), Mockito.any());
    }


    @Test
    public void testGetUpstreamDcClusterIds() {
        DbCluster clusterMeta = MetaClone.clone(getCluster("shaoy", CLUSTER_ID));
        Map<String, Set<String>> upstreamDcClusterIds = currentMetaManager.getUpstreamDcClusterIds(clusterMeta);
        Assert.assertEquals(1, upstreamDcClusterIds.size());
        Assert.assertTrue(upstreamDcClusterIds.get("sharb").contains("integration-test.fxdrcrb"));

        String newTargetName = "newTargetName";
        String newTargetMhaName = "newTargetMhaName";
        clusterMeta.getAppliers().add(new Applier().setTargetIdc("shajq").setTargetMhaName(newTargetMhaName).setTargetName(newTargetName));
        upstreamDcClusterIds = currentMetaManager.getUpstreamDcClusterIds(clusterMeta);
        Assert.assertEquals(2, upstreamDcClusterIds.size());
        Assert.assertTrue(upstreamDcClusterIds.get("sharb").contains("integration-test.fxdrcrb"));
        Assert.assertTrue(upstreamDcClusterIds.get("shajq").contains(newTargetName + "." + newTargetMhaName));
    }

    @Test(expected = IllegalArgumentException.class)
    public void updateException() {
        currentMetaManager.update(current, observable);
    }

    @After
    public void tearDown() {
        super.tearDown();
        currentMetaManager.removeObserver(observer);
        try {
            currentMetaManager.stop();
            currentMetaManager.dispose();
        } catch (Exception e) {
        }
    }

    @Test
    public void testGetAllApplier(){
        currentMetaManager = spy(new DefaultCurrentMetaManager());
        currentMetaManager.setRegionCache(regionMetaCache);
        CurrentMeta currentMeta = new CurrentMeta();
        currentMetaManager.setCurrentMeta(currentMeta);
        String clusterId = CLUSTER_ID;

        when(currentMetaManager.allClusters()).thenReturn(Sets.newHashSet(clusterId));
        Assert.assertFalse(currentMetaManager.allClusters().isEmpty());

        DbCluster cluster = getCluster("shaoy", clusterId);
        cluster.getAppliers().get(0).setMaster(true);
        currentMeta.addCluster(cluster);
        String applierRegisterKey = NameUtils.getApplierRegisterKey(clusterId, cluster.getAppliers().get(0));
        String applierBackupRegisterKey = NameUtils.getApplierBackupRegisterKey(cluster.getAppliers().get(0));
        currentMeta.setSurviveAppliers(clusterId,
                applierRegisterKey,
                cluster.getAppliers(), cluster.getAppliers().stream().filter(Applier::getMaster).findFirst().get()
        );
        when(regionMetaCache.getCluster(clusterId)).thenReturn(cluster);

        Map<String, Map<String, List<Applier>>> allMetaAppliers = currentMetaManager.getAllMetaAppliers();
        Assert.assertFalse(allMetaAppliers.isEmpty());
        List<Applier> appliers = allMetaAppliers.get(clusterId).get(applierBackupRegisterKey);
        Assert.assertTrue(appliers.stream().anyMatch(Applier::getMaster));

    }


    @Test
    public void testGetAllMessenger(){
        currentMetaManager = spy(new DefaultCurrentMetaManager());
        currentMetaManager.setRegionCache(regionMetaCache);
        CurrentMeta currentMeta = new CurrentMeta();
        currentMetaManager.setCurrentMeta(currentMeta);
        String clusterId = CLUSTER_ID;

        when(currentMetaManager.allClusters()).thenReturn(Sets.newHashSet(clusterId));
        Assert.assertFalse(currentMetaManager.allClusters().isEmpty());

        DbCluster cluster = getCluster("shaoy", clusterId);
        cluster.getMessengers().get(0).setMaster(true);
        currentMeta.addCluster(cluster);
        String registerKey = NameUtils.getMessengerRegisterKey(clusterId, cluster.getMessengers().get(0));
        currentMeta.setSurviveMessengers(clusterId,
                registerKey,
                cluster.getMessengers(), cluster.getMessengers().stream().filter(Messenger::getMaster).findFirst().get()
        );
        when(regionMetaCache.getCluster(clusterId)).thenReturn(cluster);

        Map<String, Map<String, List<Messenger>>> allMetaMessengers = currentMetaManager.getAllMetaMessengers();
        Assert.assertFalse(allMetaMessengers.isEmpty());
        List<Messenger> messengers = allMetaMessengers.get(clusterId).get(NameUtils.getMessengerDbName(registerKey));
        Assert.assertTrue(messengers.stream().anyMatch(Messenger::getMaster));
    }
    @Test
    public void testGetAllReplicator(){
        currentMetaManager = spy(new DefaultCurrentMetaManager());
        currentMetaManager.setRegionCache(regionMetaCache);
        CurrentMeta currentMeta = new CurrentMeta();
        currentMetaManager.setCurrentMeta(currentMeta);
        String clusterId = CLUSTER_ID;

        when(currentMetaManager.allClusters()).thenReturn(Sets.newHashSet(clusterId));
        Assert.assertFalse(currentMetaManager.allClusters().isEmpty());

        DbCluster cluster = getCluster("shaoy", clusterId);
        cluster.getReplicators().get(0).setMaster(true);
        currentMeta.addCluster(cluster);
        currentMeta.setSurviveReplicators(clusterId,
                cluster.getReplicators(),
                cluster.getReplicators().stream().filter(Replicator::getMaster).findFirst().get()
        );
        when(regionMetaCache.getCluster(clusterId)).thenReturn(cluster);

        Map<String, List<Replicator>> allMetaReplicator = currentMetaManager.getAllMetaReplicator();
        Assert.assertFalse(allMetaReplicator.isEmpty());
        List<Replicator> replicators = allMetaReplicator.get(clusterId);
        Assert.assertTrue(replicators.stream().anyMatch(Replicator::getMaster));

    }
}
