package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/5/13
 */
public class ReplicatorComparatorTest extends AbstractDbClusterTest {

    private ReplicatorComparator replicatorComparator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void compareEqual() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);

        replicatorComparator = new ReplicatorComparator(dbCluster, cloneDbCluster);

        replicatorComparator.compare();
        Assert.assertEquals(replicatorComparator.getAdded().size(), 0);
        Assert.assertEquals(replicatorComparator.getRemoved().size(), 0);
        Assert.assertEquals(replicatorComparator.getMofified().size(), 0);
        Assert.assertEquals(replicatorComparator.getCurrent(), replicatorComparator.getFuture());
    }

    @Test
    public void compareModifyReplicator() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        Replicator replicator = cloneDbCluster.getReplicators().get(0);
        replicator.setPort(9000);

        replicatorComparator = new ReplicatorComparator(dbCluster, cloneDbCluster);

        replicatorComparator.compare();
        Assert.assertEquals(replicatorComparator.getAdded().size(), 1);
        Assert.assertEquals(replicatorComparator.getRemoved().size(), 1);
        Assert.assertEquals(replicatorComparator.getMofified().size(), 0);
    }

    @Test
    public void compareAddReplicator() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        newReplicator.setIp("12.21.12.21");
        newReplicator.setPort(4321);
        cloneDbCluster.getReplicators().add(newReplicator);

        replicatorComparator = new ReplicatorComparator(dbCluster, cloneDbCluster);

        replicatorComparator.compare();
        Assert.assertEquals(replicatorComparator.getAdded().size(), 1);
        Assert.assertEquals(replicatorComparator.getRemoved().size(), 0);
        Assert.assertEquals(replicatorComparator.getMofified().size(), 0);
    }

    @Test
    public void compareRemoveReplicator() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        cloneDbCluster.getReplicators().clear();

        replicatorComparator = new ReplicatorComparator(dbCluster, cloneDbCluster);

        replicatorComparator.compare();
        Assert.assertEquals(replicatorComparator.getAdded().size(), 0);
        Assert.assertEquals(replicatorComparator.getRemoved().size(), 1);
        Assert.assertEquals(replicatorComparator.getMofified().size(), 0);
    }

    @Test
    public void compareModifyNotIpAndPort() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        Replicator replicator = cloneDbCluster.getReplicators().get(0);
        replicator.setMaster(true);

        replicatorComparator = new ReplicatorComparator(dbCluster, cloneDbCluster);

        replicatorComparator.compare();
        Assert.assertEquals(replicatorComparator.getAdded().size(), 0);
        Assert.assertEquals(replicatorComparator.getRemoved().size(), 0);
        Assert.assertEquals(replicatorComparator.getMofified().size(), 1);

        Set<MetaComparator> metaComparators = replicatorComparator.getMofified();
        Assert.assertEquals(metaComparators.size(), 1);

        for (MetaComparator metaComparator : metaComparators) {
            InstanceComparator instanceComparator = (InstanceComparator) metaComparator;
            Replicator replicator1 = (Replicator) instanceComparator.getCurrent();
            Replicator replicator2 = (Replicator) instanceComparator.getFuture();
            Assert.assertEquals(replicator1.getIp(), replicator2.getIp());
            Assert.assertEquals(replicator1.getPort(), replicator2.getPort());
            Assert.assertEquals(replicator1.getGtidSkip(), replicator2.getGtidSkip());
            Assert.assertEquals(replicator1.getApplierPort(), replicator2.getApplierPort());
            Assert.assertEquals(replicator1.getMaster(), false);
            Assert.assertEquals(replicator2.getMaster(), true);
        }

    }
}