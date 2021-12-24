package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.ctrip.framework.drc.manager.AllTests.DAL_CLUSTER_ID;
import static com.ctrip.framework.drc.manager.AllTests.DC;

/**
 * @Author limingdong
 * @create 2020/5/12
 */
public class ClusterComparatorTest extends AbstractDbClusterTest {

    private ClusterComparator clusterComparator;

    private DbCluster futureDbCluster;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void compareEqual() {
        Dc future = drc.getDcs().get(DC);
        Dc dcClone = MetaClone.clone(future);

        futureDbCluster = dcClone.getDbClusters().get(DAL_CLUSTER_ID);

        clusterComparator = new ClusterComparator(dbCluster, futureDbCluster);

        clusterComparator.compare();
        ReplicatorComparator replicatorComparator = clusterComparator.getReplicatorComparator();
        Assert.assertEquals(replicatorComparator.getAdded().size(), 0);
        Assert.assertEquals(replicatorComparator.getRemoved().size(), 0);
        Assert.assertEquals(replicatorComparator.getMofified().size(), 0);

        ApplierComparator applierComparator = clusterComparator.getApplierComparator();
        Assert.assertEquals(applierComparator.getAdded().size(), 0);
        Assert.assertEquals(applierComparator.getRemoved().size(), 0);
        Assert.assertEquals(applierComparator.getMofified().size(), 0);

        DbComparator dbComparator = clusterComparator.getDbComparator();
        Assert.assertEquals(dbComparator.getAdded().size(), 0);
        Assert.assertEquals(dbComparator.getRemoved().size(), 0);
        Assert.assertEquals(dbComparator.getMofified().size(), 0);
    }

    @Test
    public void compareModifyReplicator() {
        String modifyIp = "121.121.121.121";
        Dc future = drc.getDcs().get(DC);
        Dc dcClone = MetaClone.clone(future);

        futureDbCluster = dcClone.getDbClusters().get(DAL_CLUSTER_ID);

        Replicator modifyReplicator = futureDbCluster.getReplicators().get(0);
        modifyReplicator.setIp(modifyIp);

        clusterComparator = new ClusterComparator(dbCluster, futureDbCluster);

        clusterComparator.compare();
        ReplicatorComparator replicatorComparator = clusterComparator.getReplicatorComparator();
        Assert.assertEquals(replicatorComparator.getAdded().size(), 1);
        Assert.assertEquals(replicatorComparator.getRemoved().size(), 1);
        Assert.assertEquals(replicatorComparator.getMofified().size(), 0);
    }

    @Test
    public void compareAddReplicator() {
        Dc future = drc.getDcs().get(DC);
        Dc dcClone = MetaClone.clone(future);

        futureDbCluster = dcClone.getDbClusters().get(DAL_CLUSTER_ID);

        newReplicator.setIp("12.21.12.21");
        newReplicator.setPort(4321);
        futureDbCluster.addReplicator(newReplicator);

        clusterComparator = new ClusterComparator(dbCluster, futureDbCluster);

        clusterComparator.compare();
        ReplicatorComparator replicatorComparator = clusterComparator.getReplicatorComparator();
        Assert.assertEquals(replicatorComparator.getAdded().size(), 1);
        Assert.assertEquals(replicatorComparator.getRemoved().size(), 0);
        Assert.assertEquals(replicatorComparator.getMofified().size(), 0);
    }

    @Test
    public void compareRemoveReplicator() {
        Dc future = drc.getDcs().get(DC);
        Dc dcClone = MetaClone.clone(future);

        futureDbCluster = dcClone.getDbClusters().get(DAL_CLUSTER_ID);

        futureDbCluster.getReplicators().clear();

        clusterComparator = new ClusterComparator(dbCluster, futureDbCluster);

        clusterComparator.compare();
        ReplicatorComparator replicatorComparator = clusterComparator.getReplicatorComparator();
        Assert.assertEquals(replicatorComparator.getAdded().size(), 0);
        Assert.assertEquals(replicatorComparator.getRemoved().size(), 1);
        Assert.assertEquals(replicatorComparator.getMofified().size(), 0);
    }
}