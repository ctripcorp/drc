package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.meta.comparator.DcRouteComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.config.SourceProvider;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.DcComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.InstanceComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ReplicatorComparator;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Set;

import static com.ctrip.framework.drc.manager.AllTests.DAL_CLUSTER_ID;
import static com.ctrip.framework.drc.manager.AllTests.DC;

/**
 * @Author limingdong
 * @create 2020/5/12
 */
public class DefaultDcCacheTest extends AbstractDbClusterTest {

    private DefaultDcCache dcCache;

    @Mock
    private SourceProvider sourceProvider;

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private DataCenterService dataCenterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(config.getClusterRefreshMilli()).thenReturn(50);
        dcCache = new DefaultDcCache(config, sourceProvider, DC);
    }

    @Test
    public void testReplicatorAdd() throws Exception {
        init();
        final int[] updateCount = {0};

        dcCache.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                updateCount[0] = updateCount[0] + 1;
                Assert.assertTrue(args instanceof DcComparator);
                DcComparator dcComparator = (DcComparator) args;
                Assert.assertTrue(dcComparator.totalChangedCount() == 1);

                Set<MetaComparator> metaComparators =  dcComparator.getMofified();
                Assert.assertTrue(metaComparators.size() == 1);
                for (MetaComparator metaComparator : metaComparators) {
                    ClusterComparator clusterMetaComparator = (ClusterComparator) metaComparator;
                    String clusterId = clusterMetaComparator.getCurrent().getId();
                    Assert.assertEquals(DAL_CLUSTER_ID, clusterId);
                    ReplicatorComparator replicatorComparator = clusterMetaComparator.getReplicatorComparator();
                    Set<Replicator> replicatorComparators = replicatorComparator.getAdded();
                    Assert.assertTrue(replicatorComparators.size() == 1);
                    for (Replicator replicator1 : replicatorComparators) {
                        Assert.assertEquals(newReplicator, replicator1);
                        System.out.println("compare right");
                    }

                }
            }
        });

        Dc future = drc.getDcs().get(DC);
        Dc dcClone = MetaClone.clone(future);

        DbCluster dbCluster = dcClone.getDbClusters().get(DAL_CLUSTER_ID);

        dbCluster.addReplicator(newReplicator);

        when(sourceProvider.getDc(DC)).thenReturn(dcClone);
        Thread.sleep(100);

        Assert.assertEquals(1, updateCount[0]);
    }

    @Test
    public void testReplicatorRemove() throws Exception {
        init();

        final int[] updateCount = {0};

        Dc future = drc.getDcs().get(DC);
        Dc dcClone = MetaClone.clone(future);

        DbCluster dbCluster = dcClone.getDbClusters().get(DAL_CLUSTER_ID);

        Replicator removedReplicator = dbCluster.getReplicators().get(0);
        dbCluster.getReplicators().clear();

        dcCache.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                updateCount[0] = updateCount[0] + 1;
                Assert.assertTrue(args instanceof DcComparator);
                DcComparator dcComparator = (DcComparator) args;
                Assert.assertTrue(dcComparator.totalChangedCount() == 1);

                Set<MetaComparator> metaComparators =  dcComparator.getMofified();
                Assert.assertTrue(metaComparators.size() == 1);
                for (MetaComparator metaComparator : metaComparators) {
                    ClusterComparator clusterMetaComparator = (ClusterComparator) metaComparator;
                    String clusterId = clusterMetaComparator.getCurrent().getId();
                    Assert.assertEquals(DAL_CLUSTER_ID, clusterId);
                    ReplicatorComparator replicatorComparator = clusterMetaComparator.getReplicatorComparator();
                    Set<Replicator> replicatorComparators = replicatorComparator.getRemoved();
                    Assert.assertTrue(replicatorComparators.size() == 1);
                    for (Replicator replicator1 : replicatorComparators) {
                        Assert.assertEquals(removedReplicator, replicator1);
                        System.out.println("compare right");
                    }

                }
            }
        });

        when(sourceProvider.getDc(DC)).thenReturn(dcClone);
        Thread.sleep(100);

        Assert.assertEquals(1, updateCount[0]);
    }

    @Test
    public void testReplicatorModifyIpAndPort() throws Exception {
        init();
        String modifyIp = "121.121.121.121";
        final int[] updateCount = {0};

        Dc future = drc.getDcs().get(DC);
        Dc dcClone = MetaClone.clone(future);

        DbCluster dbCluster = dcClone.getDbClusters().get(DAL_CLUSTER_ID);

        Replicator modifyReplicator = dbCluster.getReplicators().get(0);
        modifyReplicator.setIp(modifyIp);

        dcCache.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                updateCount[0] = updateCount[0] + 1;
                Assert.assertTrue(args instanceof DcComparator);
                DcComparator dcComparator = (DcComparator) args;
                Assert.assertTrue(dcComparator.totalChangedCount() == 1);

                Set<MetaComparator> metaComparators =  dcComparator.getMofified();
                Assert.assertTrue(metaComparators.size() == 1);
                for (MetaComparator metaComparator : metaComparators) {
                    ClusterComparator clusterMetaComparator = (ClusterComparator) metaComparator;
                    String clusterId = clusterMetaComparator.getCurrent().getId();
                    Assert.assertEquals(DAL_CLUSTER_ID, clusterId);
                    ReplicatorComparator replicatorComparator = clusterMetaComparator.getReplicatorComparator();
                    Set<MetaComparator> comparators = replicatorComparator.getMofified();
                    Assert.assertTrue(comparators.size() == 1);
                    for (MetaComparator replicator1 : comparators) {
                        InstanceComparator instanceComparator = (InstanceComparator) replicator1;
                        Replicator r = (Replicator) instanceComparator.getCurrent();
                        Assert.assertEquals(r.getIp(), modifyIp);
                        System.out.println("compare right");
                    }

                    Set<Replicator> added = replicatorComparator.getAdded();
                    Set<Replicator> removed = replicatorComparator.getRemoved();
                    Assert.assertTrue(added.size() == 1);
                    Assert.assertTrue(removed.size() == 1);

                }
            }
        });

        when(sourceProvider.getDc(DC)).thenReturn(dcClone);
        Thread.sleep(100);

        Assert.assertEquals(1, updateCount[0]);
    }

    @Test
    public void testReplicatorModifyNotIpAndPort() throws Exception {
        init();
        final int[] updateCount = {0};
        String gtid = "uuid:id";

        Dc future = drc.getDcs().get(DC);
        Dc dcClone = MetaClone.clone(future);

        DbCluster dbCluster = dcClone.getDbClusters().get(DAL_CLUSTER_ID);

        Replicator modifyReplicator = dbCluster.getReplicators().get(0);
        modifyReplicator.setGtidSkip(gtid);

        dcCache.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                updateCount[0] = updateCount[0] + 1;
                Assert.assertTrue(args instanceof DcComparator);
                DcComparator dcComparator = (DcComparator) args;
                Assert.assertTrue(dcComparator.totalChangedCount() == 1);

                Set<MetaComparator> metaComparators =  dcComparator.getMofified();
                Assert.assertTrue(metaComparators.size() == 1);
                for (MetaComparator metaComparator : metaComparators) {
                    ClusterComparator clusterMetaComparator = (ClusterComparator) metaComparator;
                    String clusterId = clusterMetaComparator.getCurrent().getId();
                    Assert.assertEquals(DAL_CLUSTER_ID, clusterId);
                    ReplicatorComparator replicatorComparator = clusterMetaComparator.getReplicatorComparator();
                    Set<MetaComparator> comparators = replicatorComparator.getMofified();
                    Assert.assertTrue(comparators.size() == 1);
                    for (MetaComparator replicator1 : comparators) {
                        InstanceComparator instanceComparator = (InstanceComparator) replicator1;
                        Replicator r = (Replicator) instanceComparator.getCurrent();
                        Assert.assertNotEquals(r.getGtidSkip(), gtid);
                        System.out.println("compare right");
                    }

                    Set<Replicator> added = replicatorComparator.getAdded();
                    Set<Replicator> removed = replicatorComparator.getRemoved();
                    Assert.assertTrue(added.size() == 0);
                    Assert.assertTrue(removed.size() == 0);

                }
            }
        });

        when(sourceProvider.getDc(DC)).thenReturn(dcClone);
        Thread.sleep(100);

        Assert.assertEquals(1, updateCount[0]);
    }

    @Test
    public void testChangeDcMeta() throws Exception {
        init();
        Thread.sleep(50);

        final int[] updateCount = {0};

        Dc future = drc.getDcs().get(DC);
        Dc dcClone = MetaClone.clone(future);

        Dc dcClone1 = MetaClone.clone(future);
        dcClone1.getDbClusters().remove(DAL_CLUSTER_ID);

        Dc dcClone0 = MetaClone.clone(future);
        dcClone0.getDbClusters().clear();

        dcCache.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                updateCount[0] = updateCount[0] + 1;
                Assert.assertTrue(args instanceof DcComparator);
                DcComparator dcComparator = (DcComparator) args;
                Assert.assertTrue(dcComparator.totalChangedCount() == 1);

                Set<MetaComparator> metaComparators =  dcComparator.getMofified();
                Assert.assertTrue(metaComparators.size() == 1);
                for (MetaComparator metaComparator : metaComparators) {
                    ClusterComparator clusterMetaComparator = (ClusterComparator) metaComparator;
                    String clusterId = clusterMetaComparator.getCurrent().getId();
                    Assert.assertEquals(DAL_CLUSTER_ID, clusterId);
                }
            }
        });

        logger.info("try null config");
        when(sourceProvider.getDc(DC)).thenReturn(null);
        Thread.sleep(50);
        Assert.assertEquals(0, updateCount[0]);
        logger.info("try remove all");
        when(sourceProvider.getDc(DC)).thenReturn(dcClone0);
        Thread.sleep(50);
        Assert.assertEquals(0, updateCount[0]);
        logger.info("try remove 1");
        when(sourceProvider.getDc(DC)).thenReturn(dcClone1);
        Thread.sleep(50);
        Assert.assertEquals(1, updateCount[0]);
    }

    private void init() throws Exception {
        when(sourceProvider.getDc(DC)).thenReturn(current);
        dcCache.initialize();
        dcCache.start();
    }

    @Test
    public void testRouteChangeNonChange() {
        Dc current = getDc("shaoy");
        Dc future = MetaClone.clone(current);

        Observer observer = mock(Observer.class);
        dcCache.addObserver(observer);
        dcCache.checkRouteChange(current, future);
        verify(observer, never()).update(any(DcRouteComparator.class), Mockito.any());
    }

    @Test
    public void testRouteChangeNonMetaChange() {
        Dc current = getDc("shaoy");
        Dc future = MetaClone.clone(current);
        future.addRoute(new Route(1000).setTag(Route.TAG_CONSOLE).setSrcDc("fra").setDstDc("jq").setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443"));
        Observer observer = mock(Observer.class);
        dcCache.addObserver(observer);
        dcCache.checkRouteChange(current, future);
        verify(observer, never()).update(any(DcRouteComparator.class), Mockito.any());
    }

    @Test
    public void testRouteChangeWithMetaAdd() {
        Dc current = getDc("shaoy");
        Dc future = MetaClone.clone(current);
        future.addRoute(new Route(1000).setTag(Route.TAG_META).setSrcDc("shaoy").setDstDc("jq").setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443"));
        Observer observer = mock(Observer.class);
        dcCache.addObserver(observer);
        dcCache.checkRouteChange(current, future);
        // not cleared: what about the first route added
        verify(observer, never()).update(any(DcRouteComparator.class), Mockito.any());
    }

    @Test
    public void testRouteChangeWithMetaRemove() {
        Dc current = getDc("shaoy");
        Dc future = MetaClone.clone(current);
        future.getRoutes().remove(0);

        Observer observer = mock(Observer.class);
        dcCache.addObserver(observer);
        dcCache.checkRouteChange(current, future);
        verify(observer, times(1)).update(any(DcRouteComparator.class), Mockito.any());
    }

    @Test
    public void testRouteChangeWithMetaModified() {
        Dc current = getDc("shaoy");
        Dc future = MetaClone.clone(current);
        future.getRoutes().get(0).setRouteInfo("PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.2:443");

        Observer observer = mock(Observer.class);
        dcCache.addObserver(observer);
        dcCache.checkRouteChange(current, future);
        verify(observer, times(1)).update(any(DcRouteComparator.class), Mockito.any());
    }

    @Test
    public void testEmptyMetaNoChange() {
        Dc current = getDc("shaoy");
        Dc future = null;

        Observer observer = mock(Observer.class);
        dcCache.addObserver(observer);
        dcCache.changeDcMeta(current, future, System.currentTimeMillis());
        verify(observer, never()).update(any(DcComparator.class), Mockito.any());
    }
}
