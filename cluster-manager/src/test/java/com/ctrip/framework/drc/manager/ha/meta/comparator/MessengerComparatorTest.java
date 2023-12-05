package com.ctrip.framework.drc.manager.ha.meta.comparator;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * Created by jixinwang on 2022/11/15
 */
public class MessengerComparatorTest extends AbstractDbClusterTest {

    private MessengerComparator messengerComparator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void compareEqual() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);

        messengerComparator = new MessengerComparator(dbCluster, cloneDbCluster);

        messengerComparator.compare();
        Assert.assertEquals(messengerComparator.getAdded().size(), 0);
        Assert.assertEquals(messengerComparator.getRemoved().size(), 0);
        Assert.assertEquals(messengerComparator.getMofified().size(), 0);
        Assert.assertEquals(messengerComparator.getCurrent(), messengerComparator.getFuture());
    }

    @Test
    public void compareModifyNotIpAndPort() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        Messenger messenger = cloneDbCluster.getMessengers().get(0);
        messenger.setNameFilter("drc.test");

        messengerComparator = new MessengerComparator(dbCluster, cloneDbCluster);

        messengerComparator.compare();
        Assert.assertEquals(messengerComparator.getAdded().size(), 0);
        Assert.assertEquals(messengerComparator.getRemoved().size(), 0);
        Assert.assertEquals(messengerComparator.getMofified().size(), 1);

        Set<MetaComparator> metaComparators = messengerComparator.getMofified();
        Assert.assertEquals(metaComparators.size(), 1);

        for (MetaComparator metaComparator : metaComparators) {
            MessengerPropertyComparator instanceComparator = (MessengerPropertyComparator) metaComparator;
            Messenger messenger1 = (Messenger) instanceComparator.getCurrent();
            Messenger messenger2 = (Messenger) instanceComparator.getFuture();
            Assert.assertEquals(messenger1.getIp(), messenger2.getIp());
            Assert.assertEquals(messenger1.getPort(), messenger2.getPort());
            Assert.assertEquals(messenger2.getNameFilter(), "drc.test");
        }
    }

    @Test
    public void compareAddMessenger() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        newMessenger.setIp("12.21.12.21");
        newMessenger.setPort(4321);
        cloneDbCluster.getMessengers().add(newMessenger);

        messengerComparator = new MessengerComparator(dbCluster, cloneDbCluster);

        messengerComparator.compare();
        Assert.assertEquals(messengerComparator.getAdded().size(), 1);
        Assert.assertEquals(messengerComparator.getRemoved().size(), 0);
        Assert.assertEquals(messengerComparator.getMofified().size(), 0);
    }

    @Test
    public void compareRemoveMessenger() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        cloneDbCluster.getMessengers().clear();

        messengerComparator = new MessengerComparator(dbCluster, cloneDbCluster);

        messengerComparator.compare();
        Assert.assertEquals(messengerComparator.getAdded().size(), 0);
        Assert.assertEquals(messengerComparator.getRemoved().size(), 1);
        Assert.assertEquals(messengerComparator.getMofified().size(), 0);
    }
}
