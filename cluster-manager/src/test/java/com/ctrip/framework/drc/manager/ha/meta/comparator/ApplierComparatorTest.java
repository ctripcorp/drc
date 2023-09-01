package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2021/11/25
 */
public class ApplierComparatorTest extends AbstractDbClusterTest {

    private ApplierComparator applierComparator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void compareEqual() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);

        applierComparator = new ApplierComparator(dbCluster, cloneDbCluster);

        applierComparator.compare();
        Assert.assertEquals(applierComparator.getAdded().size(), 0);
        Assert.assertEquals(applierComparator.getRemoved().size(), 0);
        Assert.assertEquals(applierComparator.getMofified().size(), 0);
        Assert.assertEquals(applierComparator.getCurrent(), applierComparator.getFuture());
    }

    @Test
    public void compareModifyNotIpAndPort() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        Applier applier = cloneDbCluster.getAppliers().get(0);
        applier.setTargetIdc("shaxy");

        applierComparator = new ApplierComparator(dbCluster, cloneDbCluster);

        applierComparator.compare();
        Assert.assertEquals(applierComparator.getAdded().size(), 0);
        Assert.assertEquals(applierComparator.getRemoved().size(), 0);
        Assert.assertEquals(applierComparator.getMofified().size(), 1);

        Set<MetaComparator> metaComparators = applierComparator.getMofified();
        Assert.assertEquals(metaComparators.size(), 1);

        for (MetaComparator metaComparator : metaComparators) {
            ApplierPropertyComparator instanceComparator = (ApplierPropertyComparator) metaComparator;
            Applier applier1 = (Applier) instanceComparator.getCurrent();
            Applier applier2 = (Applier) instanceComparator.getFuture();
            Assert.assertEquals(applier1.getIp(), applier2.getIp());
            Assert.assertEquals(applier1.getPort(), applier2.getPort());
            Assert.assertEquals(applier1.getTargetMhaName(), applier2.getTargetMhaName());
            Assert.assertNotEquals(applier1.getTargetIdc(), applier2.getTargetIdc());
            Assert.assertEquals(applier2.getTargetIdc(), "shaxy");
        }
    }


    @Test
    public void compareModifyNameFilter() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        Applier applier = cloneDbCluster.getAppliers().get(0);
        applier.setNameFilter("testdb\\.testtable");

        applierComparator = new ApplierComparator(dbCluster, cloneDbCluster);

        applierComparator.compare();
        Assert.assertEquals(applierComparator.getAdded().size(), 0);
        Assert.assertEquals(applierComparator.getRemoved().size(), 0);
        Assert.assertEquals(applierComparator.getMofified().size(), 1);

        Set<MetaComparator> metaComparators = applierComparator.getMofified();
        Assert.assertEquals(metaComparators.size(), 1);

        for (MetaComparator metaComparator : metaComparators) {
            ApplierPropertyComparator propertyComparator = (ApplierPropertyComparator) metaComparator;
            Assert.assertEquals(1, propertyComparator.getAdded().size());
            Applier applier1 = (Applier) propertyComparator.getCurrent();
            Applier applier2 = (Applier) propertyComparator.getFuture();
            Assert.assertEquals(applier1.getIp(), applier2.getIp());
            Assert.assertEquals(applier1.getPort(), applier2.getPort());
            Assert.assertEquals(applier1.getTargetMhaName(), applier2.getTargetMhaName());
            Assert.assertNotEquals(applier1.getNameFilter(), applier2.getNameFilter());
            Assert.assertEquals(applier2.getNameFilter(), "testdb\\.testtable");
        }
    }

    @Test
    public void compareModifyProperty() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        Applier applier = cloneDbCluster.getAppliers().get(0);
        applier.setProperties("test.property");

        applierComparator = new ApplierComparator(dbCluster, cloneDbCluster);

        applierComparator.compare();
        Assert.assertEquals(applierComparator.getAdded().size(), 0);
        Assert.assertEquals(applierComparator.getRemoved().size(), 0);
        Assert.assertEquals(applierComparator.getMofified().size(), 1);

        Set<MetaComparator> metaComparators = applierComparator.getMofified();
        Assert.assertEquals(metaComparators.size(), 1);

        for (MetaComparator metaComparator : metaComparators) {
            ApplierPropertyComparator propertyComparator = (ApplierPropertyComparator) metaComparator;
            Assert.assertEquals(1, propertyComparator.getAdded().size());
            Applier applier1 = (Applier) propertyComparator.getCurrent();
            Applier applier2 = (Applier) propertyComparator.getFuture();
            Assert.assertEquals(applier1.getIp(), applier2.getIp());
            Assert.assertEquals(applier1.getPort(), applier2.getPort());
            Assert.assertEquals(applier1.getTargetMhaName(), applier2.getTargetMhaName());
            Assert.assertNotEquals(applier1.getProperties(), applier2.getProperties());
            Assert.assertEquals(applier2.getProperties(), "test.property");
        }
    }
}