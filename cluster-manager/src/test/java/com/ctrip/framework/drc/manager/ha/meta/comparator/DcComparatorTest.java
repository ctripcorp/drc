package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.AllTests;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public class DcComparatorTest extends AbstractDbClusterTest {

    private Dc future;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        future = MetaClone.clone(current);
    }

    @Test
    public void testEquals(){

        DcComparator dcMetaComparator = new DcComparator(current, future);
        dcMetaComparator.compare();

        Assert.assertEquals(0, dcMetaComparator.getRemoved().size());
        Assert.assertEquals(0, dcMetaComparator.getAdded().size());
        Assert.assertEquals(0, dcMetaComparator.getMofified().size());
    }

    @Test
    public void testAdded(){
        DbCluster cluster = differentCluster();
        future.addDbCluster(cluster);

        DcComparator dcComparator = new DcComparator(current, future);
        dcComparator.compare();

        Assert.assertEquals(0, dcComparator.getRemoved().size());
        Assert.assertEquals(1, dcComparator.getAdded().size());
        Assert.assertEquals(cluster, dcComparator.getAdded().toArray()[0]);
        Assert.assertEquals(0, dcComparator.getMofified().size());

    }

    @Test
    public void testDeleted(){

        DbCluster cluster = differentCluster();
        current.addDbCluster(cluster);

        DcComparator dcMetaComparator = new DcComparator(current, future);
        dcMetaComparator.compare();

        Assert.assertEquals(1, dcMetaComparator.getRemoved().size());
        Assert.assertEquals(cluster, dcMetaComparator.getRemoved().toArray()[0]);
        Assert.assertEquals(0, dcMetaComparator.getAdded().size());
        Assert.assertEquals(0, dcMetaComparator.getMofified().size());
    }

    protected DbCluster differentCluster() {
        DbCluster result = new DbCluster();
        result.setId(ADDED_ID);
        return result;
    }

    @Test
    public void testModified(){

        DbCluster dbCluster = (DbCluster) future.getDbClusters().values().toArray()[0];
        Replicator replicator = new Replicator();
        replicator.setIp(IP);
        replicator.setPort(PORT);
        dbCluster.addReplicator(replicator);

        Applier applier = new Applier();
        applier.setIp(IP);
        applier.setPort(PORT);
        applier.setTargetMhaName(AllTests.OY_MHA_NAME);
        dbCluster.addApplier(applier);

        Db db = new Db();
        db.setIp(IP);
        db.setPort(PORT);
        db.setUuid(UUID);
        Dbs dbs = dbCluster.getDbs();
        for (Db db1 : dbs.getDbs()) {
            if (db1.isMaster()) {
                db1.setMaster(false);
            } else {
                db1.setMaster(true);
            }
        }
        dbs.addDb(db);

        DcComparator dcComparator = new DcComparator(current, future);
        dcComparator.compare();

        Assert.assertEquals(0, dcComparator.getRemoved().size());
        Assert.assertEquals(0, dcComparator.getAdded().size());
        Assert.assertEquals(1, dcComparator.getMofified().size());

        ClusterComparator comparator = (ClusterComparator) dcComparator.getMofified().toArray()[0];

        Assert.assertEquals(replicator, comparator.getReplicatorComparator().getAdded().toArray()[0]);
        Assert.assertEquals(1, comparator.getReplicatorComparator().getAdded().size());

        Assert.assertEquals(applier, comparator.getApplierComparator().getAdded().toArray()[0]);
        Assert.assertEquals(1, comparator.getApplierComparator().getAdded().size());

        Assert.assertEquals(db, comparator.getDbComparator().getAdded().toArray()[0]);
        Assert.assertEquals(1, comparator.getDbComparator().getAdded().size());
        Set<MetaComparator> instanceComparator = comparator.getDbComparator().getMofified();
        Assert.assertEquals(instanceComparator.size(), 2);

        for (MetaComparator metaComparator : instanceComparator) {
            InstanceComparator instanceComparator1 = (InstanceComparator) metaComparator;
            Db current = (Db) instanceComparator1.getCurrent();
            Db future = (Db) instanceComparator1.getFuture();
            Assert.assertEquals(current.getMaster(), !future.getMaster());
        }
    }
}