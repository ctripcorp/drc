package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.ha.StateChangeHandler;
import com.ctrip.framework.drc.manager.ha.cluster.CurrentClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.SlotManager;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.DcComparator;
import com.ctrip.framework.drc.core.meta.comparator.DcRouteComparator;
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
    private DcCache dcMetaCache;

    @Mock
    private StateChangeHandler handler;

    private Executor executors = Executors.newFixedThreadPool(1);

    @Mock
    private Observer observer;

    @Mock
    private Observable observable;

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("unit_test");

    private Set<Integer> DEFAULT_SLOT = Sets.newHashSet();

    private Set<String> DEFAULT_CLUSTER = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(currentClusterServer.getServerId()).thenReturn(String.valueOf(SERVER_ID));
        when(currentClusterServer.slots()).thenReturn(DEFAULT_SLOT);

        DEFAULT_CLUSTER.add(DAL_CLUSTER_ID);
        when(dcMetaCache.getClusters()).thenReturn(DEFAULT_CLUSTER);

        when(slotManager.getSlotIdByKey(DAL_CLUSTER_ID)).thenReturn(ADDED_SLOT);
        when(dcMetaCache.getCluster(DAL_CLUSTER_ID)).thenReturn(dbCluster);

        currentMetaManager.setScheduled(scheduledExecutorService);
        currentMetaManager.setSlotCheckInterval(50);

        currentMetaManager.initialize();
        currentMetaManager.setExecutors(executors);
        currentMetaManager.start();
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
        when(dcMetaCache.getCluster(newId)).thenReturn(futureDbCluster);

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
        when(dcMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

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
        when(dcMetaCache.getCluster(CLUSTER_ID)).thenReturn(dbCluster);

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
        currentMetaManager.setDcCache(dcMetaCache);
        String clusterId = CLUSTER_ID;

        when(currentMetaManager.allClusters()).thenReturn(Sets.newHashSet(clusterId));
        Assert.assertFalse(currentMetaManager.allClusters().isEmpty());

        doReturn(Pair.from("127.0.0.1", randomPort())).when(currentMetaManager).getApplierMaster(anyString(), anyString());

        when(dcMetaCache.randomRoute(clusterId, "sharb")).thenReturn(new Route(1000).setTag(Route.TAG_META));
        when(dcMetaCache.getCluster(clusterId)).thenReturn(getCluster("shaoy", clusterId));

        currentMetaManager.addStateChangeHandler(handler);
        currentMetaManager.routeChanges();

        verify(handler, times(1)).applierMasterChanged(eq(clusterId), anyString(), Mockito.any());
    }

    @Test
    public void testGetUpstreamDcClusterIdList() {
        DbCluster clusterMeta = MetaClone.clone(getCluster("shaoy", CLUSTER_ID));
        List<Pair<String, String>> upstreamDcClusterIdList = currentMetaManager.getUpstreamDcClusterIdList(clusterMeta);
        Assert.assertEquals(1, upstreamDcClusterIdList.size());
        Assert.assertEquals("sharb", upstreamDcClusterIdList.get(0).getKey());
        Assert.assertEquals("integration-test.fxdrcrb", upstreamDcClusterIdList.get(0).getValue());

        String newTargetName = "newTargetName";
        String newTargetMhaName = "newTargetMhaName";
        clusterMeta.getAppliers().add(new Applier().setTargetIdc("shajq").setTargetMhaName(newTargetMhaName).setTargetName(newTargetName));
        upstreamDcClusterIdList = currentMetaManager.getUpstreamDcClusterIdList(clusterMeta);
        Assert.assertEquals(2, upstreamDcClusterIdList.size());
        Assert.assertEquals("sharb", upstreamDcClusterIdList.get(0).getKey());
        Assert.assertEquals("integration-test.fxdrcrb", upstreamDcClusterIdList.get(0).getValue());
        Assert.assertEquals("shajq", upstreamDcClusterIdList.get(1).getKey());
        Assert.assertEquals(newTargetName + "." + newTargetMhaName, upstreamDcClusterIdList.get(1).getValue());
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
}